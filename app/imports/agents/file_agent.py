import csv
import logging
import string
import uuid

from collections import defaultdict
from datetime import datetime
from functools import lru_cache
from io import StringIO
from typing import TYPE_CHECKING, DefaultDict, NamedTuple, Optional, Union, cast

import click
import sentry_sdk

from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobClient, BlobLeaseClient, BlobServiceClient
from pydantic import ValidationError
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import enqueue_many_retry_tasks, sync_create_many_tasks
from sqlalchemy import update
from sqlalchemy.future import select
from sqlalchemy.sql import and_, not_, or_

from app.core.config import redis, settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.enums import FileAgentType, VoucherUpdateStatuses
from app.models import Voucher, VoucherConfig, VoucherFileLog, VoucherUpdate
from app.scheduler import CronScheduler
from app.schemas import RewardUpdateSchema

logger = logging.getLogger("reward-import")

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session


class RewardUpdateRow(NamedTuple):
    data: RewardUpdateSchema
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

    def _blob_name_is_duplicate(self, db_session: "Session", file_name: str) -> bool:
        file_name = sync_run_query(
            lambda: db_session.execute(
                select(VoucherFileLog.file_name).where(
                    VoucherFileLog.file_agent_type == self.file_agent_type,  # type: ignore
                    VoucherFileLog.file_name == file_name,
                )
            ).scalar_one_or_none(),
            db_session,
        )

        return True if file_name else False

    def _log_and_capture_msg(self, msg: str) -> None:
        logger.error(msg)
        if settings.SENTRY_DSN:
            sentry_sdk.capture_message(msg)

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
            else f"{datetime.utcnow().strftime('%Y/%m/%d/%H%M')}/{src_blob_client.blob_name}",
        )
        dst_blob_client.start_copy_from_url(src_blob_client.url)  # Synchronous within the same storage account
        src_blob_client.delete_blob(lease=src_blob_lease)

    def run(self) -> None:  # pragma: no cover

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

            if self._blob_name_is_duplicate(db_session, file_name=blob.name):
                self._log_and_capture_msg(
                    f"{blob.name} is a duplicate. Moving to {settings.BLOB_ERROR_CONTAINER} for checking"
                )
                self.move_blob(settings.BLOB_ERROR_CONTAINER, blob_client, lease)
                continue

            if not blob.name.endswith(".csv"):
                self._log_and_capture_msg(
                    f"{blob.name} does not have .csv ext. Moving to {settings.BLOB_ERROR_CONTAINER} for checking"
                )
                self.move_blob(settings.BLOB_ERROR_CONTAINER, blob_client, lease)
                continue

            byte_content = blob_client.download_blob(lease=lease).readall()

            logger.debug(f"Processing blob {blob.name}.")
            try:
                self.process_csv(
                    retailer_slug=retailer_slug,
                    blob_name=blob.name,
                    blob_content=byte_content.decode("utf-8", "strict"),
                    db_session=db_session,
                )
            except BlobProcessingError as ex:
                logger.error(f"Problem processing blob {blob.name} - {ex}. Moving to {settings.BLOB_ERROR_CONTAINER}")
                self.move_blob(settings.BLOB_ERROR_CONTAINER, blob_client, lease)
                sync_run_query(lambda: db_session.rollback(), db_session)
            except UnicodeDecodeError as ex:
                logger.error(
                    f"Problem decoding blob {blob.name} (files should be utf-8 encoded) - {ex}. "
                    f"Moving to {settings.BLOB_ERROR_CONTAINER}"
                )
                self.move_blob(settings.BLOB_ERROR_CONTAINER, blob_client, lease)
                sync_run_query(lambda: db_session.rollback(), db_session)
            else:
                logger.debug(f"Archiving blob {blob.name}.")
                self.move_blob(settings.BLOB_ARCHIVE_CONTAINER, blob_client, lease)

                def add_reward_file_log() -> None:
                    db_session.add(
                        VoucherFileLog(
                            file_name=blob.name,
                            file_agent_type=self.file_agent_type,  # type: ignore
                        )
                    )
                    db_session.commit()

                sync_run_query(add_reward_file_log, db_session)


class RewardImportAgent(BlobFileAgent):
    blob_path_template = string.Template("$retailer_slug/available-rewards/")
    scheduler_name = "carina-reward-import-scheduler"

    def __init__(self) -> None:
        super().__init__()
        self.file_agent_type = FileAgentType.IMPORT

    @lru_cache()
    def reward_configs_by_reward_type_slug(self, retailer_slug: str, db_session: "Session") -> dict[str, VoucherConfig]:
        voucher_configs = sync_run_query(
            lambda: db_session.execute(select(VoucherConfig).where(VoucherConfig.retailer_slug == retailer_slug))
            .scalars()
            .all(),
            db_session,
        )
        return {voucher_config.voucher_type_slug: voucher_config for voucher_config in voucher_configs}

    def _report_pre_existing_codes(
        self, pre_existing_reward_codes: list[str], row_nums_by_code: dict[str, list[int]], blob_name: str
    ) -> None:
        msg = f"Pre-existing reward codes found in {blob_name}:\n" + "\n".join(
            [f"rows: {', '.join(map(str, row_nums_by_code[code]))}" for code in pre_existing_reward_codes]
        )
        logger.warning(msg)
        if settings.SENTRY_DSN:
            sentry_sdk.capture_message(msg)

    def _report_invalid_rows(self, invalid_rows: list[int], blob_name: str) -> None:
        if invalid_rows:
            sentry_sdk.capture_message(
                f"Invalid rows found in {blob_name}:\nrows: {', '.join(map(str, sorted(invalid_rows)))}"
            )

    def process_csv(self, retailer_slug: str, blob_name: str, blob_content: str, db_session: "Session") -> None:
        _base_path, sub_path = blob_name.split(self.blob_path_template.substitute(retailer_slug=retailer_slug))
        try:
            reward_slug, _path_remainder = sub_path.split("/", maxsplit=1)
        except ValueError as ex:
            raise BlobProcessingError(f"No reward_slug path section found ({ex})")

        try:
            voucher_config = self.reward_configs_by_reward_type_slug(retailer_slug, db_session)[reward_slug]
        except KeyError:
            raise BlobProcessingError(f"No RewardConfig found for reward_slug {reward_slug}")

        content_reader = csv.reader(StringIO(blob_content), delimiter=",", quotechar="|")
        invalid_rows: list[int] = []

        row_nums_by_code: defaultdict[str, list[int]] = defaultdict(list)
        for row_num, row in enumerate(content_reader, start=1):
            if not len(row) == 1:
                invalid_rows.append(row_num)
            elif code := row[0].strip():
                row_nums_by_code[code].append(row_num)

        db_reward_codes = sync_run_query(
            lambda: db_session.execute(
                select(Voucher.voucher_code).where(
                    or_(
                        and_(
                            Voucher.voucher_code.in_(row_nums_by_code.keys()),
                            Voucher.retailer_slug == retailer_slug,
                            Voucher.voucher_config_id == voucher_config.id,
                        ),
                        and_(Voucher.voucher_config_id != voucher_config.id, not_(Voucher.deleted)),
                        and_(Voucher.voucher_config_id == voucher_config.id, Voucher.deleted),
                    )
                )
            )
            .scalars()
            .all(),
            db_session,
        )

        self._report_invalid_rows(invalid_rows, blob_name)

        pre_existing_reward_codes = list(set(db_reward_codes) & set(row_nums_by_code.keys()))
        if pre_existing_reward_codes:
            self._report_pre_existing_codes(pre_existing_reward_codes, row_nums_by_code, blob_name)
            for pre_existing_code in pre_existing_reward_codes:
                row_nums_by_code.pop(pre_existing_code)

        new_rewards: list[Voucher] = [
            Voucher(
                voucher_code=voucher_code,
                voucher_config_id=voucher_config.id,
                retailer_slug=retailer_slug,
            )
            for voucher_code in set(row_nums_by_code)
            if voucher_code  # caters for blank lines
        ]

        def add_new_rewards() -> None:
            db_session.add_all(new_rewards)
            db_session.commit()

        sync_run_query(add_new_rewards, db_session)


class RewardUpdatesAgent(BlobFileAgent):
    blob_path_template = string.Template("$retailer_slug/reward-updates/")
    scheduler_name = "carina-rewards-update-scheduler"

    def __init__(self) -> None:
        super().__init__()
        self.file_agent_type = FileAgentType.UPDATE

    def process_csv(self, retailer_slug: str, blob_name: str, blob_content: str, db_session: "Session") -> None:
        content_reader = csv.reader(StringIO(blob_content), delimiter=",", quotechar="|")

        # This is a defaultdict(list) incase we encounter the reward code twice in one file
        reward_update_rows_by_code: defaultdict = defaultdict(list[RewardUpdateRow])
        invalid_rows: list[tuple[int, Exception]] = []
        for row_num, row in enumerate(content_reader, start=1):
            try:
                data = RewardUpdateSchema(
                    code=row[0].strip(),
                    date=row[1].strip(),
                    status=VoucherUpdateStatuses(row[2].strip()),
                )
            except (ValidationError, IndexError, ValueError) as e:
                invalid_rows.append((row_num, e))
            else:
                reward_update_rows_by_code[data.dict()["code"]].append(RewardUpdateRow(data, row_num=row_num))

        if invalid_rows:
            msg = f"Error validating RewardUpdate from CSV file {blob_name}:\n" + "\n".join(
                [f"row {row_num}: {repr(e)}" for row_num, e in invalid_rows]
            )
            logger.warning(msg)
            if settings.SENTRY_DSN:
                sentry_sdk.capture_message(msg)

        if not reward_update_rows_by_code:
            logger.warning(f"No relevant reward updates found in blob: {blob_name}")

        self._process_updates(
            db_session=db_session,
            retailer_slug=retailer_slug,
            reward_update_rows_by_code=reward_update_rows_by_code,
            blob_name=blob_name,
        )

    def _report_unknown_codes(
        self,
        reward_codes_in_file: list[str],
        db_reward_data_by_code: dict[str, dict[str, Union[str, bool]]],
        reward_update_rows_by_code: DefaultDict[str, list[RewardUpdateRow]],
        blob_name: str,
    ) -> None:
        unknown_reward_codes = list(set(reward_codes_in_file) - set(db_reward_data_by_code.keys()))
        reward_update_row_datas: list[RewardUpdateRow]
        if unknown_reward_codes:
            row_nums = []
            for unknown_reward_code in unknown_reward_codes:
                reward_update_row_datas = reward_update_rows_by_code.pop(unknown_reward_code, [])
                row_nums.extend([update_row.row_num for update_row in reward_update_row_datas])

            msg = f"Unknown reward codes found while processing {blob_name}, rows: {', '.join(map(str, row_nums))}"
            logger.warning(msg)
            if settings.SENTRY_DSN:
                sentry_sdk.capture_message(msg)

    def _process_unallocated_codes(
        self,
        db_session: "Session",
        retailer_slug: str,
        blob_name: str,
        reward_codes_in_file: list[str],
        db_reward_data_by_code: dict[str, dict[str, Union[str, bool]]],
        reward_update_rows_by_code: DefaultDict[str, list[RewardUpdateRow]],
    ) -> None:
        unallocated_reward_codes = list(
            set(reward_codes_in_file)
            & {code for code, reward_data in db_reward_data_by_code.items() if reward_data["allocated"] is False}
        )

        # Soft delete unallocated reward codes
        if unallocated_reward_codes:
            update_rows: list[RewardUpdateRow] = []
            for unallocated_reward_code in unallocated_reward_codes:
                rows = reward_update_rows_by_code.pop(unallocated_reward_code, [])
                update_rows.extend(rows)

            db_session.execute(
                update(Voucher)
                .where(Voucher.voucher_code.in_(unallocated_reward_codes), Voucher.retailer_slug == retailer_slug)
                .values(deleted=True)
            )
            msg = f"Unallocated reward codes found while processing {blob_name}:\n" + "\n".join(
                [
                    f"Reward id: {db_reward_data_by_code[row_data.data.code]['id']}"
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
        reward_update_rows_by_code: DefaultDict[str, list[RewardUpdateRow]],
        blob_name: str,
    ) -> None:

        reward_codes_in_file = list(reward_update_rows_by_code.keys())

        reward_datas = sync_run_query(
            lambda: db_session.execute(
                select(Voucher.id, Voucher.voucher_code, Voucher.allocated)
                .with_for_update()
                .where(Voucher.voucher_code.in_(reward_codes_in_file), Voucher.retailer_slug == retailer_slug)
            )
            .mappings()
            .all(),
            db_session,
        )
        # Provides a dict in the following format:
        # {'<code>': {'id': 'f2c44cf7-9d0f-45d0-b199-44a3c8b72db3', 'allocated': True}}
        db_reward_data_by_code: dict[str, dict[str, Union[str, bool]]] = {
            reward_data["voucher_code"]: {"id": str(reward_data["id"]), "allocated": reward_data["allocated"]}
            for reward_data in reward_datas
        }

        self._report_unknown_codes(reward_codes_in_file, db_reward_data_by_code, reward_update_rows_by_code, blob_name)

        self._process_unallocated_codes(
            db_session,
            retailer_slug,
            blob_name,
            reward_codes_in_file,
            db_reward_data_by_code,
            reward_update_rows_by_code,
        )

        reward_updates = []
        for voucher_code, voucher_update_rows in reward_update_rows_by_code.items():
            reward_updates.extend(
                [
                    VoucherUpdate(
                        voucher_id=uuid.UUID(cast(str, db_reward_data_by_code[voucher_code]["id"])),
                        date=reward_update_row.data.date,
                        status=reward_update_row.data.status,
                    )
                    for reward_update_row in voucher_update_rows
                ]
            )

        def add_reward_updates() -> None:
            db_session.add_all(reward_updates)
            db_session.commit()

        sync_run_query(add_reward_updates, db_session)
        self.enqueue_reward_updates(db_session, reward_updates)

    @staticmethod
    def enqueue_reward_updates(db_session: "Session", reward_updates: list[VoucherUpdate]) -> None:
        def _commit() -> None:
            db_session.commit()

        def _rollback() -> None:
            db_session.rollback()

        params_list = [
            {
                "voucher_id": reward_update.voucher.id,
                "retailer_slug": reward_update.voucher.retailer_slug,
                "date": datetime.fromisoformat(reward_update.date.isoformat()).timestamp(),
                "status": reward_update.status.value,
            }
            for reward_update in reward_updates
        ]
        tasks = sync_create_many_tasks(
            db_session, task_type_name=settings.REWARD_STATUS_ADJUSTMENT_TASK_NAME, params_list=params_list
        )
        try:
            enqueue_many_retry_tasks(
                db_session, retry_tasks_ids=[task.retry_task_id for task in tasks], connection=redis
            )
        except Exception as ex:
            sentry_sdk.capture_exception(ex)
            sync_run_query(_rollback, db_session, rollback_on_exc=False)
        else:
            for task in tasks:
                task.status = RetryTaskStatuses.IN_PROGRESS
            sync_run_query(_commit, db_session, rollback_on_exc=False)


@click.group()
def cli() -> None:  # pragma: no cover
    pass


@cli.command()
def reward_import_agent() -> None:  # pragma: no cover
    RewardImportAgent().run()


@cli.command()
def reward_updates_agent() -> None:  # pragma: no cover
    RewardUpdatesAgent().run()


if __name__ == "__main__":  # pragma: no cover
    cli()
