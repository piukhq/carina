from datetime import datetime

import click
import rq

from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import get_retry_task

from app.core.config import redis, settings
from app.db.session import SyncSessionMaker

from . import logger, send_request_with_metrics


def _process_status_adjustment(task_params: dict) -> dict:
    logger.info(f"Processing status adjustment for voucher: {task_params['voucher_id']}")
    timestamp = datetime.utcnow()
    response_audit: dict = {"timestamp": timestamp.isoformat()}

    resp = send_request_with_metrics(
        "PATCH",
        "{base_url}/bpl/loyalty/{retailer_slug}/vouchers/{voucher_id}/status".format(
            base_url=settings.POLARIS_URL,
            retailer_slug=task_params["retailer_slug"],
            voucher_id=task_params["voucher_id"],
        ),
        json={
            "status": task_params["status"],
            "date": task_params["date"],
        },
        headers={"Authorization": f"Token {settings.POLARIS_AUTH_TOKEN}"},
        timeout=(3.03, 10),
    )
    resp.raise_for_status()
    response_audit["response"] = {"status": resp.status_code, "body": resp.text}
    logger.info(f"Status adjustment succeeded for voucher: {task_params['voucher_id']}")

    return response_audit


def status_adjustment(retry_task_id: int) -> None:
    with SyncSessionMaker() as db_session:

        retry_task = get_retry_task(db_session, retry_task_id)
        retry_task.update_task(db_session, increase_attempts=True)

        response_audit = _process_status_adjustment(retry_task.params)

        retry_task.update_task(
            db_session, response_audit=response_audit, status=RetryTaskStatuses.SUCCESS, clear_next_attempt_time=True
        )


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
