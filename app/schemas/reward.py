import datetime

from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, validator

from app.enums import RewardTypeStatuses, RewardUpdateStatuses


class RewardAllocationSchema(BaseModel):  # pragma: no cover
    account_url: AnyHttpUrl


class RewardStatusSchema(BaseModel):
    status: Literal[RewardTypeStatuses.CANCELLED, RewardTypeStatuses.ENDED]

    @validator("status")
    def get_status(cls, v: str) -> RewardTypeStatuses:
        return RewardTypeStatuses(v)


class RewardUpdateSchema(BaseModel):  # pragma: no cover
    code: str
    date: str
    status: RewardUpdateStatuses

    @validator("date")
    def get_date(cls, v: str) -> datetime.date:
        return datetime.datetime.strptime(v, "%Y-%m-%d").date()
