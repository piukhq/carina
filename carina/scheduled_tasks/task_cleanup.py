from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from retry_tasks_lib.db.models import RetryTask, RetryTaskStatuses
from sqlalchemy import delete

from carina.core.config import settings
from carina.db.session import SyncSessionMaker
from carina.scheduled_tasks.scheduler import acquire_lock, cron_scheduler

from . import logger


@acquire_lock(runner=cron_scheduler)
def cleanup_old_tasks() -> None:
    """
    Delete retry_task data (including related db objects i.e task_type_key_values)
    which are greater than TASK_DATA_RETENTION_DAYS days old.
    """
    # today at midnight - 6 * 30 days (circa 6 months ago)
    tz_info = ZoneInfo(cron_scheduler.trigger_timezone)
    time_reference = datetime.now(tz=tz_info).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
        days=settings.TASK_DATA_RETENTION_DAYS
    )

    # tasks in a successful terminal state
    deleteable_task_statuses = {
        RetryTaskStatuses.SUCCESS,
        RetryTaskStatuses.CANCELLED,
        RetryTaskStatuses.REQUEUED,
        RetryTaskStatuses.CLEANUP,
    }

    logger.info("Cleaning up tasks created before %s...", time_reference.date())
    with SyncSessionMaker() as db_session:
        result = db_session.execute(
            delete(RetryTask).where(
                RetryTask.status.in_(deleteable_task_statuses),
                RetryTask.created_at < time_reference,
            )
        )
        db_session.commit()
        count = result.rowcount
    logger.info("Deleted %d tasks. ( °╭ ︿ ╮°)", count)
