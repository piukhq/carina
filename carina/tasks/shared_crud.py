from typing import TYPE_CHECKING

from sqlalchemy.future import select
from sqlalchemy.orm import joinedload

from carina.db.base_class import sync_run_query
from carina.models import RewardConfig

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def get_reward_config(db_session: "Session", reward_config_id: int) -> RewardConfig:

    reward_config: RewardConfig = sync_run_query(
        lambda: db_session.execute(
            select(RewardConfig)
            .options(joinedload(RewardConfig.retailer), joinedload(RewardConfig.fetch_type))
            .where(RewardConfig.id == reward_config_id)
        ).scalar_one(),
        db_session,
        rollback_on_exc=False,
    )

    return reward_config
