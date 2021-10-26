from datetime import datetime
from typing import TYPE_CHECKING, Optional

import click
import rq
import sentry_sdk

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import enqueue_retry_task_delay, get_retry_task
from sqlalchemy.future import select

from app.core.config import redis, settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.enums import VoucherTypeStatuses
from app.models import Voucher, VoucherConfig

from . import logger, send_request_with_metrics

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session


VOUCHER_ID = "voucher_id"
VOUCHER_CODE = "voucher_code"


def _process_issuance(task_params: dict) -> dict:
    logger.info(f"Processing allocation for voucher: {task_params['voucher_id']}")
    timestamp = datetime.utcnow()
    response_audit: dict = {"timestamp": timestamp.isoformat()}

    resp = send_request_with_metrics(
        "POST",
        task_params["account_url"],
        json={
            "voucher_code": task_params["voucher_code"],
            "issued_date": task_params["issued_date"],
            "expiry_date": task_params["expiry_date"],
            "voucher_type_slug": task_params["voucher_type_slug"],
            "voucher_id": task_params["voucher_id"],
        },
        headers={"Authorization": f"Token {settings.POLARIS_AUTH_TOKEN}"},
        timeout=(3.03, 10),
    )
    resp.raise_for_status()
    response_audit["response"] = {"status": resp.status_code, "body": resp.text}
    logger.info(f"Allocation succeeded for voucher: {task_params['voucher_id']}")

    return response_audit


def _process_and_issue_voucher(db_session: "Session", retry_task: RetryTask, task_params: dict) -> None:
    retry_task.update_task(db_session, increase_attempts=True)
    voucher_config = sync_run_query(
        lambda: db_session.execute(
            select(VoucherConfig).with_for_update().where(VoucherConfig.id == task_params["voucher_config_id"])
        )
        .scalars()
        .one(),
        db_session,
    )

    if voucher_config.status in [VoucherTypeStatuses.ENDED, VoucherTypeStatuses.CANCELLED]:
        retry_task.update_task(
            db_session, response_audit={}, status=RetryTaskStatuses.CANCELLED, clear_next_attempt_time=True
        )
    else:
        response_audit = _process_issuance(task_params)
        retry_task.update_task(
            db_session, response_audit=response_audit, status=RetryTaskStatuses.SUCCESS, clear_next_attempt_time=True
        )


def issue_voucher(retry_task_id: int) -> None:
    with SyncSessionMaker() as db_session:
        retry_task = get_retry_task(db_session, retry_task_id)
        task_params = retry_task.get_params()

        # Process the allocation if it has a voucher, else try to get a voucher - requeue that if necessary
        if "voucher_id" in task_params:
            _process_and_issue_voucher(db_session, retry_task, task_params)
        else:

            def _get_allocable_voucher() -> Optional[Voucher]:
                allocable_voucher = (
                    db_session.execute(
                        select(Voucher)
                        .with_for_update()
                        .where(
                            Voucher.voucher_config_id == task_params["voucher_config_id"],
                            Voucher.allocated == False,  # noqa
                            Voucher.deleted == False,  # noqa
                        )
                        .limit(1)
                    )
                    .scalars()
                    .first()
                )

                return allocable_voucher

            allocable_voucher: Voucher = sync_run_query(_get_allocable_voucher, db_session)
            if allocable_voucher:
                key_ids = retry_task.task_type.get_key_ids_by_name()
                task_params[VOUCHER_ID] = str(allocable_voucher.id)
                task_params[VOUCHER_CODE] = allocable_voucher.voucher_code

                def _add_voucher_to_task_values_and_set_allocated() -> None:
                    allocable_voucher.allocated = True
                    db_session.add_all(
                        retry_task.get_task_type_key_values(
                            [
                                (key_ids[VOUCHER_ID], task_params[VOUCHER_ID]),
                                (key_ids[VOUCHER_CODE], task_params[VOUCHER_CODE]),
                            ]
                        )
                    )

                    db_session.commit()

                sync_run_query(_add_voucher_to_task_values_and_set_allocated, db_session)
                _process_and_issue_voucher(db_session, retry_task, task_params)
            else:  # requeue the allocation attempt
                if retry_task.status != RetryTaskStatuses.WAITING:
                    # Only do a Sentry alert for the first allocation failure (when status is changing to WAITING)
                    with sentry_sdk.push_scope() as scope:
                        scope.fingerprint = ["{{ default }}", "{{ message }}"]
                        event_id = sentry_sdk.capture_message(
                            f"No Voucher Codes Available for VoucherConfig: {task_params['voucher_config_id']}, "
                            f"voucher type slug: {task_params['voucher_type_slug']} "
                            f"on {datetime.utcnow().strftime('%Y-%m-%d')}"
                        )
                        logger.info(f"Sentry event ID: {event_id}")

                    def _set_waiting() -> None:
                        retry_task.status = RetryTaskStatuses.WAITING.name
                        db_session.commit()

                    sync_run_query(_set_waiting, db_session)

                next_attempt_time = enqueue_retry_task_delay(
                    queue=settings.VOUCHER_ALLOCATION_TASK_QUEUE,
                    connection=redis,
                    action=issue_voucher,
                    retry_task=retry_task,
                    delay_seconds=settings.VOUCHER_ALLOCATION_REQUEUE_BACKOFF_SECONDS,
                )
                logger.info(f"Next attempt time at {next_attempt_time}")
                retry_task.update_task(db_session, next_attempt_time=next_attempt_time, increase_attempts=True)


@click.group()
def cli() -> None:  # pragma: no cover
    pass


@cli.command()
def worker(burst: bool = False) -> None:  # pragma: no cover
    from app.tasks.error_handlers import handle_voucher_issuance_error

    # placeholder for when we implement prometheus metrics
    # registry = prometheus_client.CollectorRegistry()
    # prometheus_client.multiprocess.MultiProcessCollector(registry)
    # prometheus_client.start_http_server(9100, registry=registry)

    q = rq.Queue(settings.VOUCHER_ALLOCATION_TASK_QUEUE, connection=redis)
    worker = rq.Worker(
        queues=[q],
        connection=redis,
        log_job_description=True,
        exception_handlers=[handle_voucher_issuance_error],
    )
    worker.work(burst=burst, with_scheduler=True)


if __name__ == "__main__":  # pragma: no cover
    cli()
