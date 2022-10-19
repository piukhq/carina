from typing import TYPE_CHECKING

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.utils.synchronous import cleanup_handler
from sqlalchemy.future import select

from carina.db.base_class import sync_run_query
from carina.db.session import SyncSessionMaker
from carina.fetch_reward import cleanup_reward
from carina.models import RewardConfig

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@cleanup_handler(db_session_factory=SyncSessionMaker)
def reward_issuance_cleanup_handler(retry_task: RetryTask, db_session: "Session") -> None:
    reward_config_id: int = retry_task.get_params()["reward_config_id"]
    reward_config: RewardConfig = sync_run_query(
        lambda: db_session.execute(select(RewardConfig).where(RewardConfig.id == reward_config_id)),
        db_session,
        rollback_on_exc=False,
    )
    cleanup_reward(db_session, reward_config, retry_task)
