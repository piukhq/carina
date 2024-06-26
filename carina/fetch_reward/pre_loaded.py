from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from sqlalchemy.future import select

from carina.core.config import settings
from carina.db.base_class import sync_run_query
from carina.models import Reward

from .base import BaseAgent, RewardData


class PreLoaded(BaseAgent):
    def fetch_reward(self) -> RewardData:
        """
        Fetch pre-loaded reward

        issued_date and expiry_date are set at the time of allocation

        returns (Reward data, issued_date = None, expirty_date = None, validity_days)
        """
        expiry_date: float | None = None
        validity_days = self.reward_config.load_required_fields_values()["validity_days"]

        reward = self._get_allocable_reward()
        if reward:
            self.set_agent_state_params(
                self.agent_state_params
                | {
                    self.ASSOCIATED_URL_KEY: "{base_url}/reward?{query_params}".format(
                        base_url=settings.PRE_LOADED_REWARD_BASE_URL,
                        query_params=urlencode({"retailer": self.reward_config.retailer.slug, "reward": reward.id}),
                    )
                },
                commit_changes=False,
            )
            if reward.expiry_date:
                expiry_date = datetime(
                    year=reward.expiry_date.year,
                    month=reward.expiry_date.month,
                    day=reward.expiry_date.day,
                    tzinfo=timezone.utc,
                ).timestamp()

        return RewardData(reward=reward, issued_date=None, expiry_date=expiry_date, validity_days=validity_days)

    def cleanup_reward(self) -> None:
        reward_uuid: str | None = self.retry_task.get_params().get("reward_uuid")
        if reward_uuid:
            self.update_reward_and_remove_references_from_task(reward_uuid, {"allocated": False})

    def fetch_balance(self) -> Any:  # pragma: no cover
        raise NotImplementedError

    def _get_allocable_reward(self) -> Reward | None:
        def _query() -> Reward | None:
            return (
                self.db_session.execute(
                    select(Reward)
                    .with_for_update(skip_locked=True)
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
