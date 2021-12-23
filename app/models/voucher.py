import uuid

from sqlalchemy import Boolean, Column, Date, Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.base_class import Base, TimestampMixin
from app.enums import FileAgentType, VoucherFetchType, VoucherTypeStatuses, VoucherUpdateStatuses


class Voucher(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "voucher"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    voucher_code = Column(String, nullable=False, index=True)
    allocated = Column(Boolean, default=False, nullable=False)
    deleted = Column(Boolean, default=False, nullable=False)
    voucher_config_id = Column(Integer, ForeignKey("voucher_config.id"), nullable=False)
    retailer_slug = Column(String(32), index=True, nullable=False)

    voucher_config = relationship("VoucherConfig", back_populates="vouchers")
    updates = relationship("VoucherUpdate", back_populates="voucher")

    __table_args__ = (
        UniqueConstraint(
            "voucher_code", "retailer_slug", "voucher_config_id", name="voucher_code_retailer_slug_voucher_config_unq"
        ),
    )
    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.retailer_slug}, " f"{self.voucher_code}, {self.allocated})"


class VoucherConfig(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "voucher_config"

    id = Column(Integer, primary_key=True)
    voucher_type_slug = Column(String(32), index=True, nullable=False)
    validity_days = Column(Integer, nullable=True)
    retailer_slug = Column(String(32), index=True, nullable=False)
    fetch_type = Column(Enum(VoucherFetchType), nullable=False, default=VoucherFetchType.PRE_LOADED)
    status = Column(Enum(VoucherTypeStatuses), nullable=False, default=VoucherTypeStatuses.ACTIVE)

    vouchers = relationship("Voucher", back_populates="voucher_config")

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        UniqueConstraint("voucher_type_slug", "retailer_slug", name="voucher_type_slug_retailer_slug_unq"),
    )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.retailer_slug}, " f"{self.voucher_type_slug}, {self.validity_days})"


class VoucherUpdate(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "voucher_update"

    id = Column(Integer, primary_key=True)
    voucher_id = Column(UUID(as_uuid=True), ForeignKey("voucher.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    status = Column(Enum(VoucherUpdateStatuses), nullable=False)

    voucher = relationship("Voucher", back_populates="updates")

    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.id})"


class VoucherFileLog(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "voucher_file_log"

    id = Column(Integer, primary_key=True)
    file_name = Column(String(500), index=True, nullable=False)
    file_agent_type = Column(Enum(FileAgentType), index=True, nullable=False)

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (UniqueConstraint("file_name", "file_agent_type", name="file_name_file_agent_type_unq"),)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.id})"
