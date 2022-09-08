import datetime as dt
import uuid

from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, validator

from app.enums import RewardTypeStatuses, RewardUpdateStatuses


class RewardAllocationSchema(BaseModel):  # pragma: no cover
    account_url: AnyHttpUrl
    count: int = 1
    pending_reward_id: uuid.UUID | None


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
