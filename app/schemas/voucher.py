from pydantic import BaseModel, HttpUrl


class VoucherAllocationSchema(BaseModel):  # pragma: no cover
    account_url: HttpUrl
