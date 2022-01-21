import uuid

from sqlalchemy import Boolean, Column, Date, Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.base_class import Base, TimestampMixin
from app.enums import FileAgentType, RewardTypeStatuses, RewardUpdateStatuses, RewardFetchType


class Reward(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "reward"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    code = Column(String, nullable=False, index=True)
    allocated = Column(Boolean, default=False, nullable=False)
    deleted = Column(Boolean, default=False, nullable=False)
    reward_config_id = Column(Integer, ForeignKey("reward_config.id"), nullable=False)
    retailer_slug = Column(String(32), index=True, nullable=False)

    reward_config = relationship("RewardConfig", back_populates="rewards")
    updates = relationship("RewardUpdate", back_populates="reward")

    __table_args__ = (
        UniqueConstraint(
            "code", "retailer_slug", "reward_config_id", name="code_retailer_slug_reward_config_unq"
        ),
    )
    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.retailer_slug}, " f"{self.code}, {self.allocated})"


class RewardConfig(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "reward_config"

    id = Column(Integer, primary_key=True)
    reward_slug = Column(String(32), index=True, nullable=False)
    validity_days = Column(Integer, nullable=True)
    retailer_slug = Column(String(32), index=True, nullable=False)
    fetch_type = Column(Enum(RewardFetchType), nullable=False, default=RewardFetchType.PRE_LOADED)
    status = Column(Enum(RewardTypeStatuses), nullable=False, default=RewardTypeStatuses.ACTIVE)

    rewards = relationship("Reward", back_populates="reward_config")

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        UniqueConstraint("reward_slug", "retailer_slug", name="reward_slug_retailer_slug_unq"),
    )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.retailer_slug}, " f"{self.reward_slug}, {self.validity_days})"


class RewardUpdate(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "reward_update"

    id = Column(Integer, primary_key=True)
    reward_uuid = Column(UUID(as_uuid=True), ForeignKey("reward.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    status = Column(Enum(RewardUpdateStatuses), nullable=False)

    reward = relationship("Reward", back_populates="updates")

    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.id})"


class RewardFileLog(Base, TimestampMixin):  # pragma: no cover
    __tablename__ = "reward_file_log"

    id = Column(Integer, primary_key=True)
    file_name = Column(String(500), index=True, nullable=False)
    file_agent_type = Column(Enum(FileAgentType), index=True, nullable=False)

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (UniqueConstraint("file_name", "file_agent_type", name="file_name_file_agent_type_unq"),)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.id})"
