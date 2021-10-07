import uuid

from sqlalchemy import Boolean, Column, Date, DateTime, Enum, ForeignKey, Integer, String, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import relationship

from app.db.base_class import Base, TimestampMixin
from app.enums import QueuedRetryStatuses, VoucherFetchType, VoucherTypeStatuses, VoucherUpdateStatuses


class Voucher(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "voucher"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    voucher_code = Column(String, nullable=False, index=True)
    allocated = Column(Boolean, default=False, nullable=False)
    deleted = Column(Boolean, default=False, nullable=False)
    voucher_config_id = Column(Integer, ForeignKey("voucher_config.id"), nullable=False)
    retailer_slug = Column(String(32), index=True, nullable=False)

    voucher_config = relationship("VoucherConfig", back_populates="vouchers")
    allocation = relationship("VoucherAllocation", back_populates="voucher", uselist=False)
    updates = relationship("VoucherUpdate", back_populates="voucher")

    __table_args__ = (UniqueConstraint("voucher_code", "retailer_slug", name="voucher_code_retailer_slug_unq"),)
    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.retailer_slug}, " f"{self.voucher_code}, {self.allocated})"


class VoucherConfig(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "voucher_config"

    voucher_type_slug = Column(String(32), index=True, nullable=False)
    validity_days = Column(Integer, nullable=True)
    retailer_slug = Column(String(32), index=True, nullable=False)
    fetch_type = Column(Enum(VoucherFetchType), nullable=False, default=VoucherFetchType.PRE_LOADED)
    status = Column(Enum(VoucherTypeStatuses), nullable=False, default=VoucherTypeStatuses.ACTIVE)

    vouchers = relationship("Voucher", back_populates="voucher_config")
    allocations = relationship("VoucherAllocation", back_populates="voucher_config")

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        UniqueConstraint("voucher_type_slug", "retailer_slug", name="voucher_type_slug_retailer_slug_unq"),
    )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.retailer_slug}, " f"{self.voucher_type_slug}, {self.validity_days})"


class VoucherAllocation(Base, TimestampMixin):
    __tablename__ = "voucher_allocation"

    status = Column(Enum(QueuedRetryStatuses), nullable=False, default=QueuedRetryStatuses.PENDING)
    attempts = Column(Integer, default=0, nullable=False)
    account_url = Column(String, nullable=False)
    issued_date = Column(Integer, nullable=False)
    expiry_date = Column(Integer, nullable=True)
    next_attempt_time = Column(DateTime, nullable=True)
    response_data = Column(MutableList.as_mutable(JSONB), nullable=False, default=text("'[]'::jsonb"))
    voucher_id = Column(UUID(as_uuid=True), ForeignKey("voucher.id", ondelete="CASCADE"), nullable=True)
    voucher_config_id = Column(Integer, ForeignKey("voucher_config.id", ondelete="CASCADE"), nullable=False)

    voucher = relationship("Voucher", back_populates="allocation")
    voucher_config = relationship("VoucherConfig", back_populates="allocations")

    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return f"{self.status.value.upper()} VoucherAllocation (id: {self.id})"  # type: ignore [attr-defined]


class VoucherUpdate(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "voucher_update"

    voucher_id = Column(UUID(as_uuid=True), ForeignKey("voucher.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    status = Column(Enum(VoucherUpdateStatuses), nullable=False)
    retry_status = Column(Enum(QueuedRetryStatuses), nullable=False, default=QueuedRetryStatuses.PENDING)
    attempts = Column(Integer, default=0, nullable=False)
    next_attempt_time = Column(DateTime, nullable=True)
    response_data = Column(MutableList.as_mutable(JSONB), nullable=False, default=text("'[]'::jsonb"))

    voucher = relationship("Voucher", back_populates="updates")

    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.id})"
