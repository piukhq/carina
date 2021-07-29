from pydantic import AnyHttpUrl, BaseModel


class VoucherAllocationSchema(BaseModel):  # pragma: no cover
    account_url: AnyHttpUrl
