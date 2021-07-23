from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

import httpx
import rq
import sentry_sdk

from sqlalchemy.orm.attributes import flag_modified

from app.core.config import redis, settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.enums import VoucherAllocationStatuses
from app.models import VoucherAllocation

from . import logger
from .allocation import allocate_voucher

if TYPE_CHECKING:  # pragma: no cover
    from inspect import Traceback


def requeue_allocation(allocation: VoucherAllocation) -> datetime:
    backoff_seconds = pow(settings.VOUCHER_ALLOCATION_BACKOFF_BASE, float(allocation.attempts)) * 60
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


def handle_request_exception(
    allocation: VoucherAllocation, request_exception: Union[httpx.RequestError, httpx.HTTPStatusError]
) -> Tuple[dict, Optional[VoucherAllocationStatuses], Optional[datetime]]:
    status = None
    next_attempt_time = None
    response_status = None

    terminal = False
    response_audit: Dict[str, Any] = {"error": str(request_exception), "timestamp": datetime.utcnow().isoformat()}

    if isinstance(request_exception, httpx.HTTPStatusError):
        response_status = request_exception.response.status_code
        response_audit["response"] = {
            "status": response_status,
            "body": request_exception.response.text,
        }

    logger.warning(f"Voucher allocation attempt {allocation.attempts} failed for voucher: {allocation.voucher_id}")

    if allocation.attempts < settings.VOUCHER_ALLOCATION_MAX_RETRIES:
        if response_status is None or (500 <= response_status < 600):
            next_attempt_time = requeue_allocation(allocation)
            logger.info(f"Next attempt time at {next_attempt_time}")
        else:
            terminal = True
            logger.warning(f"Received unhandlable response code ({response_status}). Stopping")
    else:
        terminal = True
        logger.warning(f"No further retries. Setting status to {VoucherAllocationStatuses.FAILED}.")
        sentry_sdk.capture_message(
            f"Voucher allocation failed (max attempts reached) for {allocation}. Stopping... {request_exception}"
        )

    if terminal:
        status = VoucherAllocationStatuses.FAILED

    return response_audit, status, next_attempt_time


def handle_voucher_allocation_error(
    job: rq.job.Job, exc_type: type, exc_value: Exception, traceback: "Traceback"
) -> None:
    response_audit = None
    next_attempt_time = None

    with SyncSessionMaker() as db_session:

        allocation = sync_run_query(
            lambda: db_session.query(VoucherAllocation).filter_by(id=job.kwargs["voucher_allocation_id"]).first(),
            db_session,
            rollback_on_exc=False,
        )

        if isinstance(exc_value, (httpx.RequestError, httpx.HTTPStatusError)):  # handle http failures specifically
            response_audit, status, next_attempt_time = handle_request_exception(allocation, exc_value)
        else:  # otherwise report to sentry and fail the task
            status = VoucherAllocationStatuses.FAILED
            sentry_sdk.capture_exception(exc_value)

        def _update_allocation() -> None:
            allocation.next_attempt_time = next_attempt_time
            flag_modified(allocation, "next_attempt_time")

            if response_audit is not None:
                allocation.response_data.append(response_audit)
                flag_modified(allocation, "response_data")

            if status is not None:
                allocation.status = status

            db_session.commit()

        sync_run_query(_update_allocation, db_session)
