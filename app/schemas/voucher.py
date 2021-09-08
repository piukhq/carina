import datetime

from pydantic import AnyHttpUrl, BaseModel, validator

from app.enums import VoucherUpdateStatuses


class VoucherAllocationSchema(BaseModel):  # pragma: no cover
    account_url: AnyHttpUrl


class VoucherUpdateSchema(BaseModel):  # pragma: no cover
    voucher_code: str
    date: str
    status: VoucherUpdateStatuses

    @validator("date")
    def get_date(cls, v: str) -> datetime.date:
        return datetime.datetime.strptime(v, "%Y-%m-%d").date()
