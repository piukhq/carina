from datetime import datetime
from typing import TYPE_CHECKING, Optional

import click
import rq
import sentry_sdk

from sqlalchemy.future import select

from app.core.config import redis, settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.enums import QueuedRetryStatuses
from app.models import TaskTypeKeyValue, Voucher, VoucherAllocation
from app.models.tasks import RetryTask
from app.retry_task_utils.synchronous import enqueue_task, get_retry_task_and_params, update_task

from . import logger, send_request_with_metrics

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session


def _process_allocation(allocation: VoucherAllocation) -> dict:
    logger.info(f"Processing allocation for voucher: {allocation.voucher_id}")
    timestamp = datetime.utcnow()
    response_audit: dict = {"timestamp": timestamp.isoformat()}

    resp = send_request_with_metrics(
        "POST",
        allocation.account_url,
        json={
            "voucher_code": allocation.voucher.voucher_code,
            "issued_date": allocation.issued_date,
            "expiry_date": allocation.expiry_date,
            "voucher_type_slug": allocation.voucher_config.voucher_type_slug,
            "voucher_id": str(allocation.voucher_id),
        },
        headers={"Authorization": f"Token {settings.POLARIS_AUTH_TOKEN}"},
        timeout=(3.03, 10),
    )
    resp.raise_for_status()
    response_audit["response"] = {"status": resp.status_code, "body": resp.text}
    logger.info(f"Allocation succeeded for voucher: {allocation.voucher_id}")

    return response_audit


def _process_and_allocate_voucher(db_session: "Session", retry_task: RetryTask) -> None:
    update_task(db_session, retry_task, increase_attempts=True)
    response_audit = _process_allocation(retry_task)
    update_task(
        db_session,
        retry_task,
        response_audit=response_audit,
        status=QueuedRetryStatuses.SUCCESS,
        clear_next_attempt_time=True,
    )


def allocate_voucher(retry_task_id: int) -> None:
    with SyncSessionMaker() as db_session:
        retry_task, task_params = get_retry_task_and_params(db_session, retry_task_id)

        # Process the allocation if it has a voucher, else try to get a voucher - requeue that if necessary
        if "voucher_id" in task_params:
            _process_and_allocate_voucher(db_session, retry_task)
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
                for key in retry_task.task_type.task_type_keys:
                    if key.name == "voucher_id":
                        voucher_id_key = key
                    elif key.name == "voucher_code":
                        voucher_code_key = key

                task_params["voucher_id"] = allocable_voucher.id
                task_params["voucher_code"] = allocable_voucher.voucher_code

                def _add_voucher_to_task_values() -> None:
                    db_session.add(
                        TaskTypeKeyValue(
                            retry_task_id=retry_task.retry_task_id,
                            task_type_key_id=voucher_id_key.id,
                            value=str(allocable_voucher.id),
                        )
                    )
                    db_session.add(
                        TaskTypeKeyValue(
                            retry_task_id=retry_task.retry_task_id,
                            task_type_key_id=voucher_code_key.id,
                            value=allocable_voucher.voucher_code,
                        )
                    )

                    db_session.commit()

                sync_run_query(_add_voucher_to_task_values, db_session)
                _process_and_allocate_voucher(db_session, retry_task)
            else:  # requeue the allocation attempt
                if retry_task.retry_status != QueuedRetryStatuses.WAITING:
                    # Only do a Sentry alert for the first allocation failure (when status is changing to WAITING)
                    sentry_sdk.capture_message(
                        f"No Voucher Codes Available for VoucherConfig: {task_params['voucher_config_id']}, "
                        f"voucher type slug: {task_params['voucher_type_slug']} "
                        f"on {datetime.utcnow().strftime('%Y-%m-%d')}"
                    )

                    def _set_waiting() -> None:
                        retry_task.retry_status = QueuedRetryStatuses.WAITING.name
                        db_session.commit()

                    sync_run_query(_set_waiting, db_session)

                next_attempt_time = enqueue_task(
                    queue=settings.VOUCHER_ALLOCATION_TASK_QUEUE,
                    action=allocate_voucher,
                    retry_task=retry_task,
                    backoff_seconds=settings.VOUCHER_ALLOCATION_REQUEUE_BACKOFF_SECONDS,
                )
                logger.info(f"Next attempt time at {next_attempt_time}")
                update_task(db_session, retry_task, next_attempt_time=next_attempt_time, increase_attempts=True)


@click.group()
def cli() -> None:  # pragma: no cover
    pass


@cli.command()
def worker(burst: bool = False) -> None:  # pragma: no cover
    from app.tasks.error_handlers import handle_voucher_allocation_error

    # placeholder for when we implement prometheus metrics
    # registry = prometheus_client.CollectorRegistry()
    # prometheus_client.multiprocess.MultiProcessCollector(registry)
    # prometheus_client.start_http_server(9100, registry=registry)

    q = rq.Queue(settings.VOUCHER_ALLOCATION_TASK_QUEUE, connection=redis)
    worker = rq.Worker(
        queues=[q],
        connection=redis,
        log_job_description=True,
        exception_handlers=[handle_voucher_allocation_error],
    )
    worker.work(burst=burst, with_scheduler=True)


if __name__ == "__main__":  # pragma: no cover
    cli()
