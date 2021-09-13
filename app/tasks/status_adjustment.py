from datetime import datetime

import click
import rq

from sqlalchemy.future import select
from sqlalchemy.orm.attributes import flag_modified

from app.core.config import redis, settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.enums import QueuedRetryStatuses
from app.models import VoucherUpdate

from . import logger, send_request_with_metrics


def _process_status_adjustment(adjustment: VoucherUpdate) -> dict:
    logger.info(f"Processing status adjustment for voucher code: {adjustment.voucher.voucher_code}")
    timestamp = datetime.utcnow()
    response_audit: dict = {"timestamp": timestamp.isoformat()}

    resp = send_request_with_metrics(
        "PATCH",
        "{base_url}/bpl/loyalty/{retailer_slug}/vouchers/{voucher_id}/status".format(
            base_url=settings.POLARIS_URL,
            retailer_slug=adjustment.voucher.retailer_slug,
            voucher_id=adjustment.voucher_id,
        ),
        json={
            "status": adjustment.status.value,  # type: ignore [attr-defined]
            "date": datetime.fromisoformat(adjustment.date.isoformat()).timestamp(),
        },
        headers={"Authorization": f"Token {settings.POLARIS_AUTH_TOKEN}"},
        timeout=(3.03, 10),
    )
    resp.raise_for_status()
    response_audit["response"] = {"status": resp.status_code, "body": resp.text}
    logger.info(f"Status adjustment succeeded for voucher code: {adjustment.voucher.voucher_code}")

    return response_audit


def status_adjustment(voucher_status_adjustment_id: int) -> None:
    with SyncSessionMaker() as db_session:

        def _get_status_adjustment() -> VoucherUpdate:
            return (
                db_session.execute(select(VoucherUpdate).where(VoucherUpdate.id == voucher_status_adjustment_id))
                .scalars()
                .one()
            )

        adjustment = sync_run_query(_get_status_adjustment, db_session)
        if adjustment.retry_status != QueuedRetryStatuses.IN_PROGRESS:
            raise ValueError(f"Incorrect state: {adjustment.retry_status}")

        def _increase_attempts() -> None:
            adjustment.attempts += 1
            db_session.commit()

        sync_run_query(_increase_attempts, db_session)
        response_audit = _process_status_adjustment(adjustment)

        def _update_status_update() -> None:
            adjustment.response_data.append(response_audit)
            flag_modified(adjustment, "response_data")
            adjustment.retry_status = QueuedRetryStatuses.SUCCESS
            adjustment.next_attempt_time = None
            db_session.commit()

        sync_run_query(_update_status_update, db_session)


@click.group()
def cli() -> None:  # pragma: no cover
    pass


@cli.command()
def worker(burst: bool = False) -> None:  # pragma: no cover
    from app.tasks.error_handlers import handle_voucher_status_adjustment_error

    # placeholder for when we implement prometheus metrics
    # registry = prometheus_client.CollectorRegistry()
    # prometheus_client.multiprocess.MultiProcessCollector(registry)
    # prometheus_client.start_http_server(9100, registry=registry)

    q = rq.Queue(settings.VOUCHER_STATUS_UPDATE_TASK_QUEUE, connection=redis)
    worker = rq.Worker(
        queues=[q],
        connection=redis,
        log_job_description=True,
        exception_handlers=[handle_voucher_status_adjustment_error],
    )
    worker.work(burst=burst, with_scheduler=True)


if __name__ == "__main__":  # pragma: no cover
    cli()