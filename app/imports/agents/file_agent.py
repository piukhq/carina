import csv
import logging
import typing

from datetime import datetime
from functools import partial
from io import StringIO
from typing import ByteString, Callable, Optional

import click
import sentry_sdk

from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient
from pydantic import ValidationError

from app import crud
from app.core.config import settings
from app.db.session import SyncSessionMaker
from app.enums import VoucherUpdateStatuses
from app.models import VoucherUpdate
from app.scheduler import CronScheduler
from app.schemas import VoucherUpdateSchema

if typing.TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger("voucher-import")


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
            voucher_config_rows = crud.get_distinct_voucher_configs(db_session)

            for voucher_config_row in voucher_config_rows:
                voucher_config_id = voucher_config_row.id
                retailer_slug = voucher_config_row.retailer_slug
                for blob in container.list_blobs(name_starts_with=f"{retailer_slug}/voucher-updates"):
                    blob_client = self.blob_service_client.get_blob_client(self.container_name, blob.name)

                    try:
                        lease = blob_client.acquire_lease(lease_duration=settings.BLOB_CLIENT_LEASE_SECONDS)
                    except HttpResponseError:
                        logger.debug(f"Skipping blob {blob.name} as we could not acquire a lease.")
                        continue

                    byte_content = blob_client.download_blob(lease=lease).readall()

                    logger.debug(f"Processing vouchers for blob {blob.name}.")
                    self.process_csv(
                        db_session=db_session,
                        retailer_slug=retailer_slug,
                        blob_name=blob.name,
                        byte_content=byte_content,
                        voucher_config_id=voucher_config_id,
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
        db_session: "Session",
        retailer_slug: str,
        blob_name: str,
        byte_content: ByteString,
        voucher_config_id: int,
    ) -> None:
        content = byte_content.decode()  # type: ignore
        content_reader = csv.reader(StringIO(content), delimiter=",", quotechar="|")
        for row_num, row in enumerate(content_reader, start=1):
            try:
                voucher_code = row[0]
                status_change_date = row[1]
                status = VoucherUpdateStatuses(row[2])
            except (IndexError, ValueError) as e:
                if settings.SENTRY_DSN:
                    sentry_sdk.capture_message(
                        f"Error parsing VoucherUpdate fields from CSV file {blob_name}, row {row_num}: {repr(e)}"
                    )
                    continue
                else:
                    raise

            voucher = crud.get_voucher(db_session=db_session, voucher_code=voucher_code, retailer_slug=retailer_slug)
            # Check the voucher exists
            if not voucher:
                if settings.SENTRY_DSN:
                    sentry_sdk.capture_message(f"Voucher Code Not Found while processing {blob_name}, row: {row_num}")
                continue

            # Check that the voucher code is allocated, soft delete if not
            if not voucher.allocated:
                crud.mark_voucher_as_deleted(db_session, voucher.id)
                if settings.SENTRY_DSN:
                    sentry_sdk.capture_message(
                        f"Voucher (id: {voucher.id}) Not Allocated while processing {blob_name}, "
                        f"row: {row_num}, status change: {status}"
                    )
                continue

            voucher_update = VoucherUpdate(
                voucher_code=voucher_code,
                date=status_change_date,
                status=status,
                voucher_config_id=voucher_config_id,
            )
            try:
                VoucherUpdateSchema.from_orm(voucher_update)
            except ValidationError as e:
                logger.error(f"Error validating VoucherUpdate from CSV file {blob_name}, row {row_num}: {repr(e)}")
                continue
            else:
                db_session.add(voucher_update)

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
