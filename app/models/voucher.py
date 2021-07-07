from sqlalchemy import Column, Integer, String, UniqueConstraint

from app.db.base_class import Base, TimestampMixin


class VoucherConfig(Base, TimestampMixin):
    __tablename__ = "voucher_config"

    voucher_type_slug = Column(String(32), index=True, nullable=False)
    validity_days = Column(Integer, nullable=True)
    retailer_slug = Column(String(32), index=True, nullable=False)

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        UniqueConstraint("voucher_type_slug", "retailer_slug", name="voucher_type_slug_retailer_slug_unq"),
    )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.retailer_slug}, " f"{self.voucher_type_slug}, {self.validity_days})"
