import csv
import logging

from collections import defaultdict, namedtuple
from datetime import datetime
from functools import partial
from io import StringIO
from typing import TYPE_CHECKING, ByteString, Callable, DefaultDict, List, Optional, Tuple, Union

import click
import sentry_sdk

from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient
from pydantic import ValidationError
from sqlalchemy import update
from sqlalchemy.future import select

from app.core.config import settings
from app.db.session import SyncSessionMaker
from app.enums import VoucherUpdateStatuses
from app.models import Voucher, VoucherUpdate
from app.scheduler import CronScheduler
from app.schemas import VoucherUpdateSchema

logger = logging.getLogger("voucher-import")

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


VoucherUpdateRow = namedtuple("VoucherUpdateRow", ["voucher_update", "row_num"])


class VoucherUpdatesAgent:
    def __init__(self) -> None:
        self.container_name = settings.BLOB_IMPORT_CONTAINER
        self.schedule = settings.BLOB_IMPORT_SCHEDULE
        self.blob_service_client = BlobServiceClient.from_connection_string(settings.BLOB_STORAGE_DSN)

    def do_import(self) -> None:  # pragma: no cover
        try:
            self.blob_service_client.create_container(self.container_name)
        except ResourceExistsError:
            pass  # this is fine

        container = self.blob_service_client.get_container_client(self.container_name)

        with SyncSessionMaker() as db_session:
            retailer_slugs = db_session.execute(select(Voucher.retailer_slug).distinct()).scalars().all()

        for retailer_slug in retailer_slugs:
            for blob in container.list_blobs(name_starts_with=f"{retailer_slug}/voucher-updates"):
                blob_client = self.blob_service_client.get_blob_client(self.container_name, blob.name)

                try:
                    lease = blob_client.acquire_lease(lease_duration=settings.BLOB_CLIENT_LEASE_SECONDS)
                except HttpResponseError:
                    if settings.SENTRY_DSN:
                        sentry_sdk.capture_message(f"Skipping blob {blob.name} as we could not acquire a lease.")
                        continue
                    else:
                        raise

                byte_content = blob_client.download_blob(lease=lease).readall()

                logger.debug(f"Processing vouchers for blob {blob.name}.")
                voucher_update_rows_by_code = self.process_csv(
                    retailer_slug=retailer_slug,
                    blob_name=blob.name,
                    byte_content=byte_content,
                )
                if voucher_update_rows_by_code:
                    with SyncSessionMaker() as db_session:
                        self.process_updates(
                            db_session=db_session,
                            retailer_slug=retailer_slug,
                            voucher_update_rows_by_code=voucher_update_rows_by_code,
                            blob_name=blob.name,
                        )
                logger.debug(f"Archiving blob {blob.name}.")
                self.archive(
                    blob.name,
                    byte_content,
                    delete_callback=partial(blob_client.delete_blob, lease=lease),
                    blob_service_client=self.blob_service_client,
                    logger=logger,
                )

    def process_csv(
        self,
        retailer_slug: str,
        blob_name: str,
        byte_content: ByteString,
    ) -> DefaultDict[str, List[VoucherUpdateRow]]:
        content = byte_content.decode()  # type: ignore
        content_reader = csv.reader(StringIO(content), delimiter=",", quotechar="|")

        # This is a defaultdict(list) incase we encounter the voucher code twice in one file
        voucher_update_rows_by_code: defaultdict = defaultdict(list[VoucherUpdateRow])
        invalid_rows: List[Tuple[int, Exception]] = []
        for row_num, row in enumerate(content_reader, start=1):
            try:
                data = VoucherUpdateSchema(
                    voucher_code=row[0].strip(),
                    date=row[1].strip(),
                    status=VoucherUpdateStatuses(row[2].strip()),
                )
            except (ValidationError, IndexError, ValueError) as e:
                invalid_rows.append((row_num, e))
            else:
                voucher_update_rows_by_code[data.dict()["voucher_code"]].append(
                    VoucherUpdateRow(VoucherUpdate(retailer_slug=retailer_slug, **data.dict()), row_num=row_num)
                )

        if invalid_rows:
            msg = f"Error validating VoucherUpdate from CSV file {blob_name}:\n" + "\n".join(
                [f"row {row_num}: {repr(e)}" for row_num, e in invalid_rows]
            )
            if settings.SENTRY_DSN:
                sentry_sdk.capture_message(msg)
            else:
                logger.warning(msg)

        if not voucher_update_rows_by_code:
            logger.warning(f"No relevant voucher updates found in blob: {blob_name}")
        return voucher_update_rows_by_code

    def _report_unknown_codes(
        self,
        voucher_codes_in_file: List[str],
        db_voucher_data_by_voucher_code: dict[str, dict[str, Union[str, bool]]],
        voucher_update_rows_by_code: DefaultDict[str, List[VoucherUpdateRow]],
        blob_name: str,
    ) -> None:
        unknown_voucher_codes = list(set(voucher_codes_in_file) - set(db_voucher_data_by_voucher_code.keys()))
        voucher_update_row_datas: List[VoucherUpdateRow]
        if unknown_voucher_codes:
            row_nums = []
            for unknown_voucher_code in unknown_voucher_codes:
                voucher_update_row_datas = voucher_update_rows_by_code.pop(unknown_voucher_code, [])
                row_nums.extend([update_row.row_num for update_row in voucher_update_row_datas])

            msg = f"Voucher Codes Not Found while processing {blob_name}, rows: {', '.join(map(str, row_nums))}"
            if settings.SENTRY_DSN:
                sentry_sdk.capture_message(msg)
            else:
                logger.warning(msg)

    def _process_unallocated_codes(
        self,
        db_session: "Session",
        retailer_slug: str,
        blob_name: str,
        voucher_codes_in_file: List[str],
        db_voucher_data_by_voucher_code: dict[str, dict[str, Union[str, bool]]],
        voucher_update_rows_by_code: DefaultDict[str, List[VoucherUpdateRow]],
    ) -> None:
        unallocated_voucher_codes = list(
            set(voucher_codes_in_file)
            & {
                voucher_code
                for voucher_code, voucher_data in db_voucher_data_by_voucher_code.items()
                if voucher_data["allocated"] is False
            }
        )

        # Soft delete unallocated voucher codes
        if unallocated_voucher_codes:
            update_rows: List[VoucherUpdateRow] = []
            for unallocated_voucher_code in unallocated_voucher_codes:
                rows = voucher_update_rows_by_code.pop(unallocated_voucher_code, [])
                update_rows.extend(rows)

            db_session.execute(
                update(Voucher)  # type: ignore
                .where(Voucher.voucher_code.in_(unallocated_voucher_codes))
                .where(Voucher.retailer_slug == retailer_slug)
                .values(deleted=True)
            )
            msg = f"Unallocated voucher codes found while processing {blob_name}:\n" + "\n".join(
                [
                    f"Voucher id: {db_voucher_data_by_voucher_code[row_data.voucher_update.voucher_code]['id']}"
                    f" row: {row_data.row_num}, status change: {row_data.voucher_update.status.value}"
                    for row_data in update_rows
                ]
            )
            if settings.SENTRY_DSN:
                sentry_sdk.capture_message(f"{blob_name} contains unallocated Voucher codes:\n\n{msg}")
            else:
                logger.warning(msg)

    def process_updates(
        self,
        db_session: "Session",
        retailer_slug: str,
        voucher_update_rows_by_code: DefaultDict[str, List[VoucherUpdateRow]],
        blob_name: str,
    ) -> None:

        voucher_codes_in_file = list(voucher_update_rows_by_code.keys())

        db_voucher_data_by_voucher_code: dict[str, dict[str, Union[str, bool]]] = {
            # Provides a dict in the following format:
            # {'<voucher-code>': {'id': 'f2c44cf7-9d0f-45d0-b199-44a3c8b72db3', 'allocated': True}}
            voucher_data["voucher_code"]: {"id": str(voucher_data["id"]), "allocated": voucher_data["allocated"]}
            for voucher_data in db_session.execute(
                select(Voucher.id, Voucher.voucher_code, Voucher.allocated)
                .with_for_update()
                .where(Voucher.voucher_code.in_(voucher_codes_in_file))
                .where(Voucher.retailer_slug == retailer_slug)
            )
            .mappings()
            .all()
        }

        self._process_unallocated_codes(
            db_session,
            retailer_slug,
            blob_name,
            voucher_codes_in_file,
            db_voucher_data_by_voucher_code,
            voucher_update_rows_by_code,
        )

        self._report_unknown_codes(
            voucher_codes_in_file, db_voucher_data_by_voucher_code, voucher_update_rows_by_code, blob_name
        )

        voucher_updates = []
        for voucher_update_rows in voucher_update_rows_by_code.values():
            voucher_updates.extend([voucher_update_row.voucher_update for voucher_update_row in voucher_update_rows])
        db_session.add_all(voucher_updates)
        db_session.commit()

    def archive(
        self,
        blob_name: str,
        blob_content: bytes,
        *,
        delete_callback: Callable,
        logger: logging.Logger,
        blob_service_client: Optional[BlobServiceClient] = None,
    ) -> None:
        if not blob_service_client:
            blob_service_client = BlobServiceClient.from_connection_string(settings.BLOB_STORAGE_DSN)

        archive_container = settings.BLOB_ARCHIVE_CONTAINER
        try:
            blob_service_client.create_container(archive_container)
        except ResourceExistsError:
            pass  # this is fine

        try:
            blob_service_client.get_blob_client(
                archive_container, f"{datetime.now().strftime('%Y/%m/%d')}/{blob_name}"
            ).upload_blob(blob_content)
        except ResourceExistsError:
            logger.warning(f"Failed to archive {blob_name} as this blob already exists in the archive.")

        delete_callback()

    def run(self) -> None:

        logger.info(f"Watching {self.container_name} for files via {self.__class__.__name__}.")

        scheduler = CronScheduler(
            name="blob-storage-import",
            schedule_fn=lambda: self.schedule,
            callback=self.callback,
            coalesce_jobs=True,
            logger=logger,
        )

        logger.debug(f"Beginning {scheduler}.")
        scheduler.run()

    def callback(self) -> None:
        self.do_import()


@click.group()
def cli() -> None:  # pragma: no cover
    pass


@cli.command()
def voucher_updates_agent() -> None:  # pragma: no cover
    VoucherUpdatesAgent().run()


if __name__ == "__main__":  # pragma: no cover
    cli()
