# this file is excluded from coverage as there is no logic to test here beyond calling a library function.
# if in the future we add any logic worth testing, please remove this file from the coveragerc ignore list.
from typing import TYPE_CHECKING

import rq

from retry_tasks_lib.utils.error_handler import handle_request_exception

from app.core.config import redis, settings
from app.db.session import SyncSessionMaker

if TYPE_CHECKING:
    from inspect import Traceback


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
def handle_retry_task_request_error(
    job: rq.job.Job, exc_type: type, exc_value: Exception, traceback: "Traceback"
) -> None:
    with SyncSessionMaker() as db_session:
        handle_request_exception(
            db_session=db_session,
            connection=redis,
            backoff_base=settings.TASK_RETRY_BACKOFF_BASE,
            max_retries=settings.TASK_MAX_RETRIES,
            job=job,
            exc_value=exc_value,
        )
