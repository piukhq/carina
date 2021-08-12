from datetime import date

from pydantic import AnyHttpUrl, BaseModel

from app.enums import VoucherImportStatuses


class VoucherAllocationSchema(BaseModel):  # pragma: no cover
    account_url: AnyHttpUrl


class VoucherImportSchema(BaseModel):  # pragma: no cover
    voucher_code: str
    date: date
    status: VoucherImportStatuses
    voucher_config_id: int

    class Config:
        orm_mode = True
        use_enum_values = False
        use_enum_names = True
