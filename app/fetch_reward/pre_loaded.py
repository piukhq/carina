from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

from sqlalchemy.future import select

from app.db.base_class import sync_run_query
from app.models import Reward

from .base import BaseAgent


class PreLoaded(BaseAgent):
    def fetch_reward(self) -> Tuple[Optional[Reward], float, float]:
        validity_days = self.reward_config.load_required_fields_values().get("validity_days", 0)
        now = datetime.now(tz=timezone.utc)
        issued = now.timestamp()
        expiry = (now + timedelta(days=validity_days)).timestamp()
        reward = self._get_allocable_reward()

        return reward, issued, expiry

    def fetch_balance(self) -> Any:  # pragma: no cover
        return NotImplementedError

    def _get_allocable_reward(self) -> Optional[Reward]:
        def _query() -> Optional[Reward]:
            return (
                self.db_session.execute(
                    select(Reward)
                    .with_for_update()
                    .where(
                        Reward.reward_config_id == self.reward_config.id,
                        Reward.allocated.is_(False),
                        Reward.deleted.is_(False),
                    )
                    .limit(1)
                )
                .scalars()
                .first()
            )

        return sync_run_query(_query, self.db_session)
