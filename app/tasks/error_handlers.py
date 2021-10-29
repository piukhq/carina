# this file is excluded from coverage as there is no logic to test here beyond alling a library function.
# if in the future we add any logic worth testing, please remove this file from the coveragerc ignore list.
from typing import TYPE_CHECKING

import rq

from retry_tasks_lib.utils.error_handler import handle_request_exception

from app.core.config import redis, settings
from app.db.session import SyncSessionMaker

if TYPE_CHECKING:
    from inspect import Traceback


def handle_voucher_issuance_error(
    job: rq.job.Job, exc_type: type, exc_value: Exception, traceback: "Traceback"
) -> None:

    with SyncSessionMaker() as db_session:
        handle_request_exception(
            db_session=db_session,
            connection=redis,
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
            backoff_base=settings.VOUCHER_STATUS_UPDATE_BACKOFF_BASE,
            max_retries=settings.VOUCHER_STATUS_UPDATE_MAX_RETRIES,
            job=job,
            exc_value=exc_value,
        )
