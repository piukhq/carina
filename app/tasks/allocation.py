from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional

import click
import rq
import sentry_sdk

from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified

from app.core.config import redis, settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.enums import QueuedRetryStatuses
from app.models import Voucher, VoucherAllocation
from app.version import __version__

from . import logger, send_request_with_metrics

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session


if settings.SENTRY_DSN:  # pragma: no cover
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        environment=settings.SENTRY_ENV,
        release=__version__,
        traces_sample_rate=settings.SENTRY_TRACES_SAMPLE_RATE,
    )


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


def _process_and_allocate_voucher(db_session: "Session", allocation: VoucherAllocation) -> None:
    def _increase_attempts() -> None:
        allocation.attempts += 1
        db_session.commit()

    sync_run_query(_increase_attempts, db_session)
    response_audit = _process_allocation(allocation)

    def _update_allocation() -> None:
        allocation.response_data.append(response_audit)
        flag_modified(allocation, "response_data")
        allocation.status = QueuedRetryStatuses.SUCCESS  # type: ignore
        allocation.next_attempt_time = None
        db_session.commit()

    sync_run_query(_update_allocation, db_session)


def _requeue_allocation(allocation: VoucherAllocation, backoff_seconds: int) -> datetime:
    q = rq.Queue(settings.VOUCHER_ALLOCATION_TASK_QUEUE, connection=redis)
    next_attempt_time = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(seconds=backoff_seconds)
    job = q.enqueue_at(  # requires rq worker --with-scheduler
        next_attempt_time,
        allocate_voucher,
        voucher_allocation_id=allocation.id,
        failure_ttl=60 * 60 * 24 * 7,  # 1 week
    )

    logger.info(f"Requeued task for execution at {next_attempt_time.isoformat()}: {job}")
    return next_attempt_time


def allocate_voucher(voucher_allocation_id: int) -> None:
    with SyncSessionMaker() as db_session:

        def _get_allocation() -> VoucherAllocation:
            return (
                db_session.execute(
                    select(VoucherAllocation)
                    .options(
                        joinedload(VoucherAllocation.voucher),
                        joinedload(VoucherAllocation.voucher_config),
                    )
                    .filter_by(id=voucher_allocation_id)
                )
                .scalars()
                .one()
            )

        allocation = sync_run_query(_get_allocation, db_session)
        if allocation.status not in ([QueuedRetryStatuses.IN_PROGRESS, QueuedRetryStatuses.WAITING]):
            raise ValueError(f"Incorrect state: {allocation.status}")

        def _get_allocable_voucher() -> Optional[Voucher]:
            allocable_voucher = (
                db_session.execute(
                    select(Voucher)
                    .with_for_update()
                    .where(
                        Voucher.voucher_config_id == allocation.voucher_config_id,
                        Voucher.allocated == False,  # noqa
                        Voucher.deleted == False,  # noqa
                    )
                    .limit(1)
                )
                .scalars()
                .first()
            )

            return allocable_voucher

        # Process the allocation if it has a voucher, else try to get a voucher - requeue that if necessary
        if allocation.voucher_id:
            _process_and_allocate_voucher(db_session, allocation)
        else:
            allocable_voucher = sync_run_query(_get_allocable_voucher, db_session)
            if allocable_voucher:

                def _set_allocable_voucher() -> None:
                    allocation.voucher = allocable_voucher
                    db_session.commit()

                sync_run_query(_set_allocable_voucher, db_session)
                _process_and_allocate_voucher(db_session, allocation)
            else:  # requeue the allocation attempt
                if allocation.status != QueuedRetryStatuses.WAITING:
                    # Only do a Sentry alert for the first allocation failure (when status is changing to WAITING)
                    sentry_sdk.capture_message(
                        f"No Voucher Codes Available for retailer: {allocation.voucher_config.retailer_slug}, "
                        f"voucher type slug: {allocation.voucher_config.voucher_type_slug}"
                    )

                    def _set_waiting() -> None:
                        allocation.status = QueuedRetryStatuses.WAITING
                        db_session.commit()

                    sync_run_query(_set_waiting, db_session)

                next_attempt_time = _requeue_allocation(
                    allocation=allocation, backoff_seconds=settings.VOUCHER_ALLOCATION_REQUEUE_BACKOFF_SECONDS
                )
                logger.info(f"Next attempt time at {next_attempt_time}")

                def _update_allocation() -> None:
                    allocation.attempts += 1
                    allocation.next_attempt_time = next_attempt_time
                    db_session.commit()

                sync_run_query(_update_allocation, db_session)


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
