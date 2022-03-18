from datetime import datetime, timedelta, timezone

import rq
import sentry_sdk

from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.enums import RetryTaskStatuses
from sqlalchemy import func
from sqlalchemy.future import select
from sqlalchemy.sql.expression import and_, or_

from app.core.config import settings
from app.db.session import SyncSessionMaker

from . import logger
from .prometheus import task_statuses


class RetryTaskWorker(rq.Worker):
    """
    Uses rq.Worker.run_maintenance_tasks as a hook to update prometheus metrics
    """

    def run_maintenance_tasks(self) -> None:
        super().run_maintenance_tasks()
        self.update_metrics()

    @staticmethod
    def update_metrics() -> None:
        """
        Query the database to find tasks that need reporting
        """
        logger.info(f"Updating {task_statuses} metrics ...")
        try:
            with SyncSessionMaker() as db_session:
                now = datetime.now(tz=timezone.utc)
                res = (
                    db_session.execute(
                        select(TaskType.name, RetryTask.status, func.count(RetryTask.retry_task_id).label("count"))
                        .join(TaskType)
                        .where(
                            or_(
                                and_(
                                    RetryTask.status == RetryTaskStatuses.PENDING,
                                    RetryTask.updated_at < now - timedelta(hours=1),
                                ),
                                and_(
                                    RetryTask.status == RetryTaskStatuses.IN_PROGRESS,
                                    or_(
                                        RetryTask.next_attempt_time.is_(None),
                                        RetryTask.next_attempt_time < now,
                                    ),
                                    RetryTask.updated_at < now - timedelta(hours=1),
                                ),
                                and_(
                                    RetryTask.status == RetryTaskStatuses.WAITING,
                                    RetryTask.updated_at < now - timedelta(days=2),
                                ),
                            )
                        )
                        .group_by(TaskType.name, RetryTask.status)
                    )
                    .mappings()
                    .all()
                )
                for row in res:
                    task_statuses.labels(
                        app=settings.PROJECT_NAME,
                        task_name=row["name"],
                        status=RetryTaskStatuses(row["status"]).name,
                    ).set(int(row["count"]))

        except Exception as ex:  # pylint: disable=broad-except
            sentry_sdk.capture_exception(ex)
