from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Union

import requests
import rq
import sentry_sdk

from sqlalchemy.orm.attributes import flag_modified

from app.core.config import redis, settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.enums import QueuedRetryStatuses
from app.models import VoucherAllocation, VoucherUpdate

from . import logger
from .allocation import allocate_voucher
from .status_adjustment import status_adjustment

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


def requeue_status_adjustment(adjustment: VoucherUpdate) -> datetime:
    backoff_seconds = pow(settings.VOUCHER_STATUS_UPDATE_BACKOFF_BASE, float(adjustment.attempts)) * 60
    q = rq.Queue(settings.VOUCHER_STATUS_UPDATE_TASK_QUEUE, connection=redis)
    next_attempt_time = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(seconds=backoff_seconds)
    job = q.enqueue_at(  # requires rq worker --with-scheduler
        next_attempt_time,
        status_adjustment,
        voucher_allocation_id=adjustment.id,
        failure_ttl=60 * 60 * 24 * 7,  # 1 week
    )

    logger.info(f"Requeued task for execution at {next_attempt_time.isoformat()}: {job}")
    return next_attempt_time


def handle_request_exception(
    queued_retry: Union[VoucherAllocation, VoucherUpdate], request_exception: requests.RequestException
) -> Tuple[dict, Optional[QueuedRetryStatuses], Optional[datetime]]:
    status = None
    next_attempt_time = None
    requeue: Callable[[Union[VoucherAllocation, VoucherUpdate]], datetime]

    if isinstance(queued_retry, VoucherUpdate):
        subject = "Voucher status adjustment"
        requeue = requeue_status_adjustment
    elif isinstance(queued_retry, VoucherAllocation):
        subject = "Voucher allocation"
        requeue = requeue_allocation
    else:
        raise ValueError("invalid queued retry type.")

    terminal = False
    response_audit: Dict[str, Any] = {"error": str(request_exception), "timestamp": datetime.utcnow().isoformat()}

    if request_exception.response is not None:
        response_audit["response"] = {
            "status": request_exception.response.status_code,
            "body": request_exception.response.text,
        }

    logger.warning(f"{subject} attempt {queued_retry.attempts} failed for voucher: {queued_retry.voucher_id}")

    if queued_retry.attempts < settings.VOUCHER_ALLOCATION_MAX_RETRIES:
        if request_exception.response is None or (500 <= request_exception.response.status_code < 600):
            next_attempt_time = requeue(queued_retry)
            logger.info(f"Next attempt time at {next_attempt_time}")
        else:
            terminal = True
            logger.warning(f"Received unhandlable response code ({request_exception.response.status_code}). Stopping")
    else:
        terminal = True
        logger.warning(f"No further retries. Setting status to {QueuedRetryStatuses.FAILED}.")
        sentry_sdk.capture_message(
            f"{subject} failed (max attempts reached) for {queued_retry}. Stopping... {request_exception}"
        )

    if terminal:
        status = QueuedRetryStatuses.FAILED

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

        if isinstance(exc_value, requests.RequestException):  # handle http failures specifically
            response_audit, status, next_attempt_time = handle_request_exception(allocation, exc_value)
        else:  # otherwise report to sentry and fail the task
            status = QueuedRetryStatuses.FAILED
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


def handle_voucher_status_adjustment_error(
    job: rq.job.Job, exc_type: type, exc_value: Exception, traceback: "Traceback"
) -> None:
    response_audit = None
    next_attempt_time = None

    with SyncSessionMaker() as db_session:

        adjustment = sync_run_query(
            lambda: db_session.query(VoucherUpdate).filter_by(id=job.kwargs["voucher_status_adjustment_id"]).first(),
            db_session,
            rollback_on_exc=False,
        )

        if isinstance(exc_value, requests.RequestException):  # handle http failures specifically
            response_audit, status, next_attempt_time = handle_request_exception(adjustment, exc_value)
        else:  # otherwise report to sentry and fail the task
            status = QueuedRetryStatuses.FAILED
            sentry_sdk.capture_exception(exc_value)

        def _update_status_adjustment() -> None:
            adjustment.next_attempt_time = next_attempt_time
            flag_modified(adjustment, "next_attempt_time")

            if response_audit is not None:
                adjustment.response_data.append(response_audit)
                flag_modified(adjustment, "response_data")

            if status is not None:
                adjustment.retry_status = status

            db_session.commit()

        sync_run_query(_update_status_adjustment, db_session)
