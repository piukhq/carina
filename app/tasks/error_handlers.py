from typing import TYPE_CHECKING

import rq

from retry_task_lib.utils.error_handler import handle_request_exception

from app.core.config import redis, settings
from app.db.session import SyncSessionMaker

from . import logger
from .allocation import issue_voucher
from .status_adjustment import status_adjustment

if TYPE_CHECKING:  # pragma: no cover
    from inspect import Traceback


def handle_voucher_issuance_error(
    job: rq.job.Job, exc_type: type, exc_value: Exception, traceback: "Traceback"
) -> None:

    with SyncSessionMaker() as db_session:
        handle_request_exception(
            db_session=db_session,
            queue=settings.VOUCHER_ALLOCATION_TASK_QUEUE,
            connection=redis,
            action=issue_voucher,
            backoff_base=settings.VOUCHER_ALLOCATION_BACKOFF_BASE,
            max_retries=settings.VOUCHER_ALLOCATION_MAX_RETRIES,
            job=job,
            exc_value=exc_value,
        )


def handle_voucher_status_adjustment_error(
    job: rq.job.Job, exc_type: type, exc_value: Exception, traceback: "Traceback"
) -> None:

    with SyncSessionMaker() as db_session:
        handle_request_exception(
            db_session=db_session,
            queue=settings.VOUCHER_STATUS_UPDATE_TASK_QUEUE,
            connection=redis,
            action=status_adjustment,
            backoff_base=settings.VOUCHER_STATUS_UPDATE_BACKOFF_BASE,
            max_retries=settings.VOUCHER_STATUS_UPDATE_MAX_RETRIES,
            job=job,
            exc_value=exc_value,
        )
