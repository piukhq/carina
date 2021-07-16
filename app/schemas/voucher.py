from pydantic import BaseModel


class VoucherAllocationSchema(BaseModel):  # pragma: no cover
    account_url: str
