import datetime

from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, validator

from app.enums import VoucherTypeStatuses, VoucherUpdateStatuses


class VoucherAllocationSchema(BaseModel):  # pragma: no cover
    account_url: AnyHttpUrl


class VoucherStatusSchema(BaseModel):
    status: Literal[VoucherTypeStatuses.CANCELLED, VoucherTypeStatuses.ENDED]

    @validator("status")
    def get_status(cls, v: str) -> VoucherTypeStatuses:
        return VoucherTypeStatuses(v)


class VoucherUpdateSchema(BaseModel):  # pragma: no cover
    voucher_code: str
    date: str
    status: VoucherUpdateStatuses

    @validator("date")
    def get_date(cls, v: str) -> datetime.date:
        return datetime.datetime.strptime(v, "%Y-%m-%d").date()
