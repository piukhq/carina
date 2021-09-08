import csv
import logging
import string
import uuid

from collections import defaultdict
from datetime import datetime
from functools import lru_cache
from io import StringIO
from typing import TYPE_CHECKING, DefaultDict, NamedTuple, Optional, Tuple, Union, cast

import click
import sentry_sdk

from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobClient, BlobLeaseClient, BlobServiceClient
from pydantic import ValidationError
from sqlalchemy import update
from sqlalchemy.future import select

from app.core.config import settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.enums import VoucherUpdateStatuses
from app.models import Voucher, VoucherConfig, VoucherUpdate
from app.scheduler import CronScheduler
from app.schemas import VoucherUpdateSchema

logger = logging.getLogger("voucher-import")

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session


class VoucherUpdateRow(NamedTuple):
    data: VoucherUpdateSchema
    row_num: int


class BlobProcessingError(Exception):
    pass


class BlobFileAgent:
    blob_path_template = string.Template("")  # Override in subclass
    scheduler_name = "carina-blob-file-agent"

    def __init__(self) -> None:
        self.container_name = settings.BLOB_IMPORT_CONTAINER
        self.schedule = settings.BLOB_IMPORT_SCHEDULE
        blob_client_logger = logging.getLogger("blob-client")
        blob_client_logger.setLevel(settings.BLOB_IMPORT_LOGGING_LEVEL)
        self.blob_service_client = BlobServiceClient.from_connection_string(
            settings.BLOB_STORAGE_DSN, logger=blob_client_logger
        )

        try:
            self.blob_service_client.create_container(self.container_name)
        except ResourceExistsError:
            pass  # this is fine
        self.container_client = self.blob_service_client.get_container_client(self.container_name)

    def retailer_slugs(self, db_session: "Session") -> list[str]:
        return sync_run_query(
            lambda: db_session.execute(select(VoucherConfig.retailer_slug).distinct()).scalars().all(), db_session
        )

    def process_csv(
        self, retailer_slug: str, blob_name: str, blob_content: str, db_session: "Session"
    ) -> None:  # pragma: no cover
        raise NotImplementedError

    def move_blob(
        self,
        destination_container: str,
        src_blob_client: "BlobClient",
        src_blob_lease: "BlobLeaseClient",
        *,
        dst_blob_name: Optional[str] = None,
    ) -> None:

        try:
            self.blob_service_client.create_container(destination_container)
        except ResourceExistsError:
            pass  # this is fine

        dst_blob_client = self.blob_service_client.get_blob_client(
            destination_container,
            dst_blob_name
            if dst_blob_name is not None
            else f"{datetime.now().strftime('%Y/%m/%d/%H%M')}/{src_blob_client.blob_name}",
        )
        dst_blob_client.start_copy_from_url(src_blob_client.url)  # Synchronous within the same storage account
        src_blob_client.delete_blob(lease=src_blob_lease)

    def run(self) -> None:

        logger.info(f"Watching {self.container_name} for files via {self.__class__.__name__}.")

        scheduler = CronScheduler(
            name=self.scheduler_name,
            schedule_fn=lambda: self.schedule,
            callback=self.do_import,
            coalesce_jobs=True,
            logger=logger,
        )

        logger.debug(f"Beginning {scheduler}.")
        scheduler.run()

    def do_import(self) -> None:  # pragma: no cover
        with SyncSessionMaker() as db_session:
            for retailer_slug in self.retailer_slugs(db_session):
                self.process_blobs(retailer_slug, db_session)

    def process_blobs(self, retailer_slug: str, db_session: "Session") -> None:
        for blob in self.container_client.list_blobs(
            name_starts_with=self.blob_path_template.substitute(retailer_slug=retailer_slug)
        ):
            blob_client = self.blob_service_client.get_blob_client(self.container_name, blob.name)

            try:
                lease = blob_client.acquire_lease(lease_duration=settings.BLOB_CLIENT_LEASE_SECONDS)
            except HttpResponseError:
                msg = f"Skipping blob {blob.name} as we could not acquire a lease."
                logger.warning(msg)
                if settings.SENTRY_DSN:
                    sentry_sdk.capture_message(msg)
                continue

            if not blob.name.endswith(".csv"):
                msg = f"{blob.name} does not have .csv ext. Moving to {settings.BLOB_ERROR_CONTAINER} for checking"
                logger.error(msg)
                if settings.SENTRY_DSN:
                    sentry_sdk.capture_message(msg)
                self.move_blob(settings.BLOB_ERROR_CONTAINER, blob_client, lease)
                continue

            byte_content = blob_client.download_blob(lease=lease).readall()

            logger.debug(f"Processing blob {blob.name}.")
            try:
                self.process_csv(
                    retailer_slug=retailer_slug,
                    blob_name=blob.name,
                    blob_content=byte_content.decode(),
                    db_session=db_session,
                )
            except BlobProcessingError as ex:
                logger.error(f"Problem processing blob {blob.name} - {ex}. Moving to {settings.BLOB_ERROR_CONTAINER}")
                self.move_blob(settings.BLOB_ERROR_CONTAINER, blob_client, lease)
                sync_run_query(lambda: db_session.rollback(), db_session)
            else:
                logger.debug(f"Archiving blob {blob.name}.")
                self.move_blob(settings.BLOB_ARCHIVE_CONTAINER, blob_client, lease)


class VoucherImportAgent(BlobFileAgent):
    blob_path_template = string.Template("$retailer_slug/available-vouchers/")
    scheduler_name = "carina-voucher-import-scheduler"

    @lru_cache()
    def voucher_configs_by_voucher_type_slug(self, retailer_slug: str) -> dict[str, VoucherConfig]:
        with SyncSessionMaker() as db_session:
            voucher_configs = sync_run_query(
                lambda: db_session.execute(select(VoucherConfig).where(VoucherConfig.retailer_slug == retailer_slug))
                .scalars()
                .all(),
                db_session,
            )
        return {voucher_config.voucher_type_slug: voucher_config for voucher_config in voucher_configs}

    def _report_pre_existing_codes(
        self, pre_existing_voucher_codes: list[str], row_nums_by_code: dict[str, list[int]], blob_name: str
    ) -> None:
        msg = f"Pre-existing voucher codes found in {blob_name}:\n" + "\n".join(
            [f"rows {', '.join(map(str, row_nums_by_code[code]))}" for code in pre_existing_voucher_codes]
        )
        logger.warning(msg)
        if settings.SENTRY_DSN:
            sentry_sdk.capture_message(msg)

    def process_csv(self, retailer_slug: str, blob_name: str, blob_content: str, db_session: "Session") -> None:
        _base_path, sub_path = blob_name.split(self.blob_path_template.substitute(retailer_slug=retailer_slug))
        try:
            voucher_type_slug, _path_remainder = sub_path.split("/", maxsplit=1)
        except ValueError as ex:
            raise BlobProcessingError(f"No voucher_type_slug path section found ({ex})")

        try:
            voucher_config = self.voucher_configs_by_voucher_type_slug(retailer_slug)[voucher_type_slug]
        except KeyError:
            raise BlobProcessingError(f"No VoucherConfig found for voucher_type_slug {voucher_type_slug}")

        content_reader = csv.reader(StringIO(blob_content), delimiter=",", quotechar="|")
        invalid_rows: list[int] = []

        row_nums_by_code: defaultdict[str, list[int]] = defaultdict(list)
        for row_num, row in enumerate(content_reader, start=1):
            if not len(row) == 1:
                invalid_rows.append(row_num)
            elif code := row[0].strip():
                row_nums_by_code[code].append(row_num)

        db_voucher_codes = sync_run_query(
            lambda: db_session.execute(
                select(Voucher.voucher_code).where(
                    Voucher.voucher_code.in_(row_nums_by_code.keys()), Voucher.retailer_slug == retailer_slug
                )
            )
            .scalars()
            .all(),
            db_session,
        )

        pre_existing_voucher_codes = list(set(db_voucher_codes) & set(row_nums_by_code.keys()))
        if pre_existing_voucher_codes:
            self._report_pre_existing_codes(pre_existing_voucher_codes, row_nums_by_code, blob_name)
            for pre_existing_code in pre_existing_voucher_codes:
                row_nums_by_code.pop(pre_existing_code)

        new_vouchers: list[Voucher] = [
            Voucher(
                voucher_code=voucher_code,
                voucher_config_id=voucher_config.id,
                retailer_slug=retailer_slug,
            )
            for voucher_code in set(row_nums_by_code)
            if voucher_code  # caters for blank lines
        ]

        def add_new_vouchers() -> None:
            db_session.add_all(new_vouchers)
            db_session.commit()

        sync_run_query(add_new_vouchers, db_session)


class VoucherUpdatesAgent(BlobFileAgent):
    blob_path_template = string.Template("$retailer_slug/voucher-updates/")
    scheduler_name = "carina-voucher-update-scheduler"

    def process_csv(self, retailer_slug: str, blob_name: str, blob_content: str, db_session: "Session") -> None:
        content_reader = csv.reader(StringIO(blob_content), delimiter=",", quotechar="|")

        # This is a defaultdict(list) incase we encounter the voucher code twice in one file
        voucher_update_rows_by_code: defaultdict = defaultdict(list[VoucherUpdateRow])
        invalid_rows: list[Tuple[int, Exception]] = []
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
                voucher_update_rows_by_code[data.dict()["voucher_code"]].append(VoucherUpdateRow(data, row_num=row_num))

        if invalid_rows:
            msg = f"Error validating VoucherUpdate from CSV file {blob_name}:\n" + "\n".join(
                [f"row {row_num}: {repr(e)}" for row_num, e in invalid_rows]
            )
            logger.warning(msg)
            if settings.SENTRY_DSN:
                sentry_sdk.capture_message(msg)

        if not voucher_update_rows_by_code:
            logger.warning(f"No relevant voucher updates found in blob: {blob_name}")

        self._process_updates(
            db_session=db_session,
            retailer_slug=retailer_slug,
            voucher_update_rows_by_code=voucher_update_rows_by_code,
            blob_name=blob_name,
        )

    def _report_unknown_codes(
        self,
        voucher_codes_in_file: list[str],
        db_voucher_data_by_voucher_code: dict[str, dict[str, Union[str, bool]]],
        voucher_update_rows_by_code: DefaultDict[str, list[VoucherUpdateRow]],
        blob_name: str,
    ) -> None:
        unknown_voucher_codes = list(set(voucher_codes_in_file) - set(db_voucher_data_by_voucher_code.keys()))
        voucher_update_row_datas: list[VoucherUpdateRow]
        if unknown_voucher_codes:
            row_nums = []
            for unknown_voucher_code in unknown_voucher_codes:
                voucher_update_row_datas = voucher_update_rows_by_code.pop(unknown_voucher_code, [])
                row_nums.extend([update_row.row_num for update_row in voucher_update_row_datas])

            msg = f"Unknown voucher codes found while processing {blob_name}, rows: {', '.join(map(str, row_nums))}"
            logger.warning(msg)
            if settings.SENTRY_DSN:
                sentry_sdk.capture_message(msg)

    def _process_unallocated_codes(
        self,
        db_session: "Session",
        retailer_slug: str,
        blob_name: str,
        voucher_codes_in_file: list[str],
        db_voucher_data_by_voucher_code: dict[str, dict[str, Union[str, bool]]],
        voucher_update_rows_by_code: DefaultDict[str, list[VoucherUpdateRow]],
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
            update_rows: list[VoucherUpdateRow] = []
            for unallocated_voucher_code in unallocated_voucher_codes:
                rows = voucher_update_rows_by_code.pop(unallocated_voucher_code, [])
                update_rows.extend(rows)

            db_session.execute(  # this is retried via the _process_updates top level method
                update(Voucher)  # type: ignore
                .where(Voucher.voucher_code.in_(unallocated_voucher_codes), Voucher.retailer_slug == retailer_slug)
                .values(deleted=True)
            )
            msg = f"Unallocated voucher codes found while processing {blob_name}:\n" + "\n".join(
                [
                    f"Voucher id: {db_voucher_data_by_voucher_code[row_data.data.voucher_code]['id']}"
                    f" row: {row_data.row_num}, status change: {row_data.data.status.value}"
                    for row_data in update_rows
                ]
            )
            logger.warning(msg)
            if settings.SENTRY_DSN:
                sentry_sdk.capture_message(msg)

    def _process_updates(
        self,
        db_session: "Session",
        retailer_slug: str,
        voucher_update_rows_by_code: DefaultDict[str, list[VoucherUpdateRow]],
        blob_name: str,
    ) -> None:

        voucher_codes_in_file = list(voucher_update_rows_by_code.keys())

        voucher_datas = sync_run_query(
            lambda: db_session.execute(
                select(Voucher.id, Voucher.voucher_code, Voucher.allocated)
                .with_for_update()
                .where(Voucher.voucher_code.in_(voucher_codes_in_file), Voucher.retailer_slug == retailer_slug)
            )
            .mappings()
            .all(),
            db_session,
        )
        # Provides a dict in the following format:
        # {'<voucher-code>': {'id': 'f2c44cf7-9d0f-45d0-b199-44a3c8b72db3', 'allocated': True}}
        db_voucher_data_by_voucher_code: dict[str, dict[str, Union[str, bool]]] = {
            voucher_data["voucher_code"]: {"id": str(voucher_data["id"]), "allocated": voucher_data["allocated"]}
            for voucher_data in voucher_datas
        }

        self._report_unknown_codes(
            voucher_codes_in_file, db_voucher_data_by_voucher_code, voucher_update_rows_by_code, blob_name
        )

        self._process_unallocated_codes(
            db_session,
            retailer_slug,
            blob_name,
            voucher_codes_in_file,
            db_voucher_data_by_voucher_code,
            voucher_update_rows_by_code,
        )

        voucher_updates = []
        for voucher_code, voucher_update_rows in voucher_update_rows_by_code.items():
            voucher_updates.extend(
                [
                    VoucherUpdate(
                        voucher_id=uuid.UUID(cast(str, db_voucher_data_by_voucher_code[voucher_code]["id"])),
                        date=voucher_update_row.data.date,
                        status=voucher_update_row.data.status,
                    )
                    for voucher_update_row in voucher_update_rows
                ]
            )

        def add_voucher_updates() -> None:
            db_session.add_all(voucher_updates)
            db_session.commit()

        sync_run_query(add_voucher_updates, db_session)


@click.group()
def cli() -> None:  # pragma: no cover
    pass


@cli.command()
def voucher_import_agent() -> None:  # pragma: no cover
    VoucherImportAgent().run()


@cli.command()
def voucher_updates_agent() -> None:  # pragma: no cover
    VoucherUpdatesAgent().run()


if __name__ == "__main__":  # pragma: no cover
    cli()
