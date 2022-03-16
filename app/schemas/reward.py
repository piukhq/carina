import datetime as dt

from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, validator

from app.enums import RewardTypeStatuses, RewardUpdateStatuses


class RewardAllocationSchema(BaseModel):  # pragma: no cover
    account_url: AnyHttpUrl


class RewardStatusSchema(BaseModel):
    status: Literal[RewardTypeStatuses.CANCELLED, RewardTypeStatuses.ENDED]

    @validator("status")
    def get_status(cls, v: str) -> RewardTypeStatuses:  # pylint: disable=no-self-argument,no-self-use
        return RewardTypeStatuses(v)


class RewardUpdateSchema(BaseModel):  # pragma: no cover
    code: str
    date: str
    status: RewardUpdateStatuses

    @validator("date")
    def get_date(cls, v: str) -> dt.date:  # pylint: disable=no-self-argument,no-self-use
        return dt.datetime.strptime(v, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).date()
