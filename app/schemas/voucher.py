from datetime import date

from pydantic import AnyHttpUrl, BaseModel

from app.enums import VoucherUpdateStatuses


class VoucherAllocationSchema(BaseModel):  # pragma: no cover
    account_url: AnyHttpUrl


class VoucherUpdateSchema(BaseModel):  # pragma: no cover
    voucher_code: str
    date: date
    status: VoucherUpdateStatuses
    voucher_config_id: int

    class Config:
        orm_mode = True
        use_enum_values = False
        use_enum_names = True
