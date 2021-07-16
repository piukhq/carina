from sqlalchemy import Boolean, Column, DateTime, Enum, ForeignKey, Integer, String, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import relationship

from app.db.base_class import Base, TimestampMixin
from app.enums import VoucherAllocationStatuses


class VoucherConfig(Base, TimestampMixin):
    __tablename__ = "voucher_config"

    voucher_type_slug = Column(String(32), index=True, nullable=False)
    validity_days = Column(Integer, nullable=True)
    retailer_slug = Column(String(32), index=True, nullable=False)

    vouchers = relationship("Voucher", backref="voucher_config")
    allocations = relationship("VoucherAllocation", backref="voucher_config")

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        UniqueConstraint("voucher_type_slug", "retailer_slug", name="voucher_type_slug_retailer_slug_unq"),
    )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.retailer_slug}, " f"{self.voucher_type_slug}, {self.validity_days})"


class Voucher(Base):
    __tablename__ = "voucher"

    # placeholder for david's code


class VoucherAllocation(Base, TimestampMixin):
    __tablename__ = "voucher_allocation"

    status = Column(Enum(VoucherAllocationStatuses), nullable=False, default=VoucherAllocationStatuses.PENDING)
    attempts = Column(Integer, default=0, nullable=False)
    account_url = Column(String, nullable=False)
    issued_date = Column(Integer, nullable=False)
    expiry_date = Column(Integer, nullable=True)
    next_attempt_time = Column(DateTime, nullable=True)
    response_data = Column(MutableList.as_mutable(JSONB), nullable=False, default=text("'[]'::jsonb"))
    voucher_id = Column(UUID(as_uuid=True), ForeignKey("voucher.id", ondelete="CASCADE"), nullable=True)
    voucher_config_id = Column(Integer, ForeignKey("voucher_config.id", ondelete="CASCADE"), nullable=False)

    voucher = relationship("Voucher", back_populates="allocation", lazy="joined")
    voucher_config = relationship("VoucherConfig", back_populates="allocations", lazy="joined")

    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return f"{self.status.value.upper()} VoucherAllocation (id: {self.id})"  # type: ignore [attr-defined]
