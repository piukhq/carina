from typing import TYPE_CHECKING

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.utils.synchronous import cleanup_handler

from carina.db.session import SyncSessionMaker
from carina.fetch_reward import cleanup_reward
from carina.tasks.shared_crud import get_reward_config

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@cleanup_handler(db_session_factory=SyncSessionMaker)
def reward_issuance_cleanup_handler(retry_task: RetryTask, db_session: "Session") -> None:
    reward_config = get_reward_config(db_session, retry_task.get_params()["reward_config_id"])
    cleanup_reward(db_session, reward_config, retry_task)
