from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from app.activity_utils.schemas import RewardStatusDataSchema
from app.core.config import settings

from . import logger


def _try_parse_account_url_path(account_url_path: str) -> str:
    try:
        return str(UUID(account_url_path.split("/accounts/", 1)[1].split("/rewards", 1)[0]))
    except (IndexError, ValueError):
        logger.warning(
            "failed to extract account_holder_uuid from path '%s', using whole path as user_id", account_url_path
        )
        return account_url_path


class ActivityType(Enum):
    REWARD_STATUS = f"activity.{settings.PROJECT_NAME}.reward.status"

    @classmethod
    def get_reward_status_activity_data(
        cls,
        *,
        account_url_path: str,
        retailer_slug: str,
        reward_slug: str,
        activity_timestamp: float,
        reward_uuid: str,
        pending_reward_id: str | None = None,
    ) -> dict:
        data_payload = {"new_status": "issued", "reward_slug": reward_slug}

        if pending_reward_id:
            reason = "Pending Reward converted"
            data_payload["original_status"] = "pending"
            data_payload["pending_reward_id"] = pending_reward_id
        else:
            reason = "Reward goal met"

        return {
            "type": cls.REWARD_STATUS.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": datetime.fromtimestamp(activity_timestamp, tz=timezone.utc),
            "summary": f'{retailer_slug} Reward "issued"',
            "reasons": [reason],
            "activity_identifier": reward_uuid,
            "user_id": _try_parse_account_url_path(account_url_path),
            "associated_value": "issued",
            "retailer": retailer_slug,
            "campaigns": [],
            "data": RewardStatusDataSchema(**data_payload).dict(exclude_unset=True),
        }
