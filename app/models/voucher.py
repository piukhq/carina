import uuid

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.base_class import Base, TimestampMixin


class VoucherRetailer(Base):  # pragma: no cover
    __tablename__ = "voucher_retailer"

    id = Column(Integer, primary_key=True, index=True)
    retailer_slug = Column(String(32), index=True, unique=True, nullable=False)
    vouchers = relationship("Voucher", back_populates="voucher_retailer", lazy="joined")
    voucher_configs = relationship("VoucherConfig", back_populates="voucher_retailer", lazy="joined")

    __mapper_args__ = {"eager_defaults": True}


class Voucher(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "voucher"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    voucher_code = Column(String, nullable=False, index=True)
    allocated = Column(Boolean, default=False, nullable=False)
    voucher_config_id = Column(Integer, ForeignKey("voucher_config.id", ondelete="CASCADE"), nullable=False)
    voucher_config = relationship("VoucherConfig", back_populates="vouchers")
    voucher_retailer_id = Column(Integer, ForeignKey("voucher_retailer.id"), nullable=False)
    voucher_retailer = relationship("VoucherRetailer", back_populates="vouchers")
    __table_args__ = (
        UniqueConstraint("voucher_code", "voucher_retailer_id", name="voucher_code_voucher_retailer_unq"),
    )

    __mapper_args__ = {"eager_defaults": True}


class VoucherConfig(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "voucher_config"

    voucher_type_slug = Column(String(32), index=True, nullable=False)
    validity_days = Column(Integer, nullable=True)
    vouchers = relationship("Voucher", back_populates="voucher_config", lazy="joined")
    voucher_retailer_id = Column(Integer, ForeignKey("voucher_retailer.id"), nullable=True)
    voucher_retailer = relationship("VoucherRetailer", back_populates="voucher_configs")

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        UniqueConstraint("voucher_type_slug", "voucher_retailer_id", name="voucher_type_slug_voucher_retailer_unq"),
    )

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.voucher_retailer.retailer_slug}, "
            f"{self.voucher_type_slug}, {self.validity_days})"
        )
