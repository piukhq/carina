from typing import TYPE_CHECKING

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import retryable_task
from sqlalchemy import update

from app.core.config import settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.models import Reward, RewardConfig

from . import logger
from .prometheus import tasks_run_total

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
@retryable_task(db_session_factory=SyncSessionMaker)
def delete_unallocated_rewards(retry_task: RetryTask, db_session: "Session") -> None:
    tasks_run_total.labels(app=settings.PROJECT_NAME, task_name=settings.DELETE_UNALLOCATED_REWARDS_TASK_NAME).inc()
    task_params = retry_task.get_params()

    def _delete_rewards() -> int:
        result = db_session.execute(
            update(Reward)
            .where(
                Reward.allocated.is_(False),
                Reward.retailer_id == task_params["retailer_id"],
                Reward.reward_config_id == RewardConfig.id,
                RewardConfig.reward_slug == task_params["reward_slug"],
            )
            .values(deleted=True)
            .execution_options(synchronize_session=False)
        )
        retry_task.status = RetryTaskStatuses.SUCCESS
        retry_task.next_attempt_time = None
        db_session.commit()
        return result.rowcount

    deleted = sync_run_query(_delete_rewards, db_session)
    logger.info(f"Deleted {deleted} campaign rewards")
