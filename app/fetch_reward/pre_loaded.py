from datetime import datetime, timedelta
from typing import Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from app.crud import get_allocable_reward
from app.models import Reward, RewardConfig


async def get_reward(db_session: AsyncSession, reward_config: RewardConfig) -> Tuple[Optional[Reward], float, float]:
    validity_days = reward_config.load_required_fields_values().get("validity_days", 0)
    now = datetime.utcnow()
    issued = now.timestamp()
    expiry = (now + timedelta(days=validity_days)).timestamp()
    reward = await get_allocable_reward(db_session, reward_config)

    return reward, issued, expiry
