import uuid

import yaml

from sqlalchemy import BigInteger, Boolean, Column, Date, Enum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from carina.db.base_class import Base, TimestampMixin
from carina.enums import FileAgentType, RewardCampaignStatuses, RewardTypeStatuses, RewardUpdateStatuses


class Reward(Base, TimestampMixin):
    __tablename__ = "reward"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    code = Column(String, nullable=False, index=True)
    allocated = Column(Boolean, default=False, nullable=False)
    deleted = Column(Boolean, default=False, nullable=False)
    reward_config_id = Column(Integer, ForeignKey("reward_config.id"), nullable=False)
    retailer_id = Column(Integer, ForeignKey("retailer.id", ondelete="CASCADE"), nullable=False)
    expiry_date = Column(Date, nullable=True)
    reward_file_log_id = Column(Integer, ForeignKey("reward_file_log.id"), nullable=True)

    reward_config = relationship("RewardConfig", back_populates="rewards")
    retailer = relationship("Retailer", back_populates="rewards")
    updates = relationship("RewardUpdate", back_populates="reward")
    reward_file_log = relationship("RewardFileLog", back_populates="rewards")

    __table_args__ = (
        UniqueConstraint("code", "retailer_id", "reward_config_id", name="code_retailer_reward_config_unq"),
    )
    __mapper_args__ = {"eager_defaults": True}

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.retailer.slug}, " f"{self.code}, {self.allocated})"


class RewardConfig(Base, TimestampMixin):
    __tablename__ = "reward_config"

    id = Column(Integer, primary_key=True)
    reward_slug = Column(String(32), index=True, nullable=False)
    retailer_id = Column(Integer, ForeignKey("retailer.id", ondelete="CASCADE"), nullable=False)
    fetch_type_id = Column(Integer, ForeignKey("fetch_type.id", ondelete="CASCADE"), nullable=False)
    status = Column(Enum(RewardTypeStatuses), nullable=False, default=RewardTypeStatuses.ACTIVE)
    required_fields_values = Column(Text, nullable=True)

    rewards = relationship("Reward", back_populates="reward_config")
    retailer = relationship("Retailer", back_populates="reward_configs")
    fetch_type = relationship("FetchType", back_populates="reward_configs")

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (UniqueConstraint("reward_slug", "retailer_id", name="reward_slug_retailer_unq"),)

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.retailer.slug}, " f"{self.reward_slug})"

    def load_required_fields_values(self) -> dict:
        if self.required_fields_values in ["", None]:
            return {}

        return yaml.safe_load(self.required_fields_values)


class RewardUpdate(Base, TimestampMixin):
    __tablename__ = "reward_update"

    id = Column(Integer, primary_key=True)
    reward_uuid = Column(UUID(as_uuid=True), ForeignKey("reward.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    status = Column(Enum(RewardUpdateStatuses), nullable=False)

    reward = relationship("Reward", back_populates="updates")

    __mapper_args__ = {"eager_defaults": True}

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.id})"


class RewardFileLog(Base, TimestampMixin):
    __tablename__ = "reward_file_log"

    id = Column(Integer, primary_key=True)
    file_name = Column(String(500), index=True, nullable=False)
    file_agent_type = Column(Enum(FileAgentType), index=True, nullable=False)

    rewards = relationship("Reward", back_populates="reward_file_log")

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (UniqueConstraint("file_name", "file_agent_type", name="file_name_file_agent_type_unq"),)

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.id})"


IDEMPOTENCY_TOKEN_REWARD_ALLOCATION_UNQ_CONSTRAINT_NAME = "idempotency_token_reward_allocation_unq"


class Allocation(Base, TimestampMixin):
    __tablename__ = "allocation"

    id = Column(BigInteger, primary_key=True)
    idempotency_token = Column(String, nullable=False)
    count = Column(Integer, nullable=False)
    account_url = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint("idempotency_token", name=IDEMPOTENCY_TOKEN_REWARD_ALLOCATION_UNQ_CONSTRAINT_NAME),
    )


class RewardCampaign(Base, TimestampMixin):
    __tablename__ = "reward_campaign"

    id = Column(Integer, primary_key=True)
    reward_slug = Column(String(32), index=True, nullable=False)
    campaign_slug = Column(String(100), index=True, nullable=False)
    retailer_id = Column(Integer, ForeignKey("retailer.id", ondelete="CASCADE"), nullable=False)
    campaign_status = Column(Enum(RewardCampaignStatuses), nullable=False)

    retailer = relationship("Retailer", back_populates="reward_campaigns")

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (UniqueConstraint("campaign_slug", "retailer_id", name="campaign_slug_retailer_unq"),)

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.retailer.slug}, " f"{self.campaign_slug})"


CAMPAIGN_RETAILER_UNQ_CONSTRAINT_NAME = "campaign_slug_retailer_unq"
