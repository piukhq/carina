import datetime as dt
import uuid

from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, validator
from pydantic.types import constr

from carina.enums import RewardCampaignStatuses, RewardTypeStatuses, RewardUpdateStatuses


class ActivityMetadataSchema(BaseModel):
    pending_reward_id: uuid.UUID | None
    reason: str | None


class RewardAllocationSchema(BaseModel):  # pragma: no cover
    account_url: AnyHttpUrl
    count: int = 1
    campaign_slug: str
    pending_reward_id: uuid.UUID | None
    activity_metadata: ActivityMetadataSchema | None


class RewardCampaignSchema(BaseModel):  # pragma: no cover
    campaign_slug: constr(min_length=1, strip_whitespace=True)  # type: ignore  # noqa
    status: RewardCampaignStatuses


class RewardStatusSchema(BaseModel):
    status: Literal[RewardTypeStatuses.CANCELLED, RewardTypeStatuses.ENDED]

    @validator("status")
    @classmethod
    def get_status(cls, v: str) -> RewardTypeStatuses:
        return RewardTypeStatuses(v)


class RewardUpdateSchema(BaseModel):  # pragma: no cover
    code: str
    date: str
    status: RewardUpdateStatuses

    @validator("date")
    @classmethod
    def get_date(cls, v: str) -> dt.date:
        return dt.datetime.strptime(v, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).date()
