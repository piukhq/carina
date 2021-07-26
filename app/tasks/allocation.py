import logging

from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import click
import requests
import rq

from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified
from tenacity import retry
from tenacity.before import before_log
from tenacity.retry import retry_if_exception_type, retry_if_result
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed

from app.core.config import redis, settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.enums import VoucherAllocationStatuses
from app.models import VoucherAllocation

from . import logger


def update_metrics_hook(response: requests.Response, *args: Any, **kwargs: Any) -> None:
    # placeholder for when we add prometheus metrics
    pass


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    reraise=True,
    before=before_log(logger, logging.INFO),
    retry_error_callback=lambda retry_state: retry_state.outcome.result(),
    retry=retry_if_result(lambda resp: 501 <= resp.status_code < 600)
    | retry_if_exception_type(requests.RequestException),
)
def send_request_with_metrics(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
    timeout: Tuple[float, int],
) -> requests.Response:

    return requests.request(
        method, url, hooks={"response": update_metrics_hook}, headers=headers, json=json, timeout=timeout
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
        if allocation.status != VoucherAllocationStatuses.IN_PROGRESS:
            raise ValueError(f"Incorrect state: {allocation.status}")

        def _increase_attempts() -> None:
            allocation.attempts += 1
            db_session.commit()

        sync_run_query(_increase_attempts, db_session)
        response_audit = _process_allocation(allocation)

        def _update_allocation() -> None:
            allocation.response_data.append(response_audit)
            flag_modified(allocation, "response_data")
            allocation.status = VoucherAllocationStatuses.SUCCESS
            allocation.next_attempt_time = None
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
