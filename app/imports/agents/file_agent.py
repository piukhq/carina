import csv
import logging
import typing

from functools import partial
from io import StringIO
from typing import ByteString, Callable, Optional

import click
import pendulum

from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient
from pydantic import ValidationError

from app.core.config import settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.enums import VoucherImportStatuses
from app.models import VoucherConfig, VoucherImport
from app.scheduler import CronScheduler
from app.schemas import VoucherImportSchema

if typing.TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger("file-agent-import")


class BlobFileAgent:
    container_name = settings.BLOB_IMPORT_CONTAINER
    schedule = settings.BLOB_IMPORT_SCHEDULE
    blob_service_client = BlobServiceClient.from_connection_string(settings.BLOB_STORAGE_DSN)

    def do_import(self) -> None:
        try:
            self.blob_service_client.create_container(self.container_name)
        except ResourceExistsError:
            pass  # this is fine

        container = self.blob_service_client.get_container_client(self.container_name)

        with SyncSessionMaker() as db_session:
            voucher_config_rows: list = sync_run_query(
                lambda: db_session.query(VoucherConfig.id, VoucherConfig.retailer_slug).distinct(
                    VoucherConfig.retailer_slug
                ),
                db_session,
                rollback_on_exc=False,
            )

            for voucher_config_row in voucher_config_rows:
                retailer_slug = dict(voucher_config_row)["retailer_slug"]
                voucher_config_id = dict(voucher_config_row)["id"]
                for blob in container.list_blobs(name_starts_with=f"{retailer_slug}/"):
                    blob_client = self.blob_service_client.get_blob_client(self.container_name, blob.name)

                    try:
                        lease = blob_client.acquire_lease(lease_duration=60)
                    except HttpResponseError:
                        logger.debug(f"Skipping blob {blob.name} as we could not acquire a lease.")
                        continue

                    byte_content = blob_client.download_blob(lease=lease).readall()

                    logger.debug(f"Processing vouchers for blob {blob.name}.")
                    self.process_csv(
                        db_session=db_session,
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
        self, db_session: "Session", blob_name: str, byte_content: ByteString, voucher_config_id: int
    ) -> None:
        content = byte_content.decode()  # type: ignore
        content_reader = csv.reader(StringIO(content), delimiter=",", quotechar="|")
        for row in content_reader:
            try:
                voucher_import = VoucherImport(
                    voucher_code=row[0],
                    date=row[1],
                    status=VoucherImportStatuses(row[2]),
                    voucher_config_id=voucher_config_id,
                )
            except (IndexError, KeyError, ValueError) as e:
                logger.error(f"Error creating VoucherImport from CSV file {blob_name} - {e} - CSV row: {row}")
                continue

            try:
                VoucherImportSchema.from_orm(voucher_import)
            except ValidationError as e:
                logger.error(f"Error validating VoucherImport from CSV file {blob_name} - {e} - CSV row: {row}")
                continue
            else:
                db_session.add(voucher_import)

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
                archive_container, f"{pendulum.today().format('YYYY/MM/DD')}/{blob_name}"
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
def blobimport() -> None:  # pragma: no cover
    blob_file_agent = BlobFileAgent()
    blob_file_agent.run()


if __name__ == "__main__":  # pragma: no cover
    cli()
