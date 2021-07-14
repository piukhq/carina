import uuid

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.base_class import Base, TimestampMixin


class Voucher(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "voucher"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    voucher_code = Column(String, nullable=False, unique=True, index=True)
    allocated = Column(Boolean, default=False, nullable=False)
    voucher_config_id = Column(Integer, ForeignKey("voucher_config.id", ondelete="CASCADE"), nullable=False)
    voucher_config = relationship("VoucherConfig", back_populates="vouchers")

    __mapper_args__ = {"eager_defaults": True}


class VoucherConfig(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "voucher_config"

    voucher_type_slug = Column(String(32), index=True, nullable=False)
    validity_days = Column(Integer, nullable=True)
    retailer_slug = Column(String(32), index=True, nullable=False)
    vouchers = relationship("Voucher", back_populates="voucher_config", lazy="joined")

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        UniqueConstraint("voucher_type_slug", "retailer_slug", name="voucher_type_slug_retailer_slug_unq"),
    )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.retailer_slug}, " f"{self.voucher_type_slug}, {self.validity_days})"
