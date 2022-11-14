import yaml

from sqlalchemy import Column, Enum, ForeignKey, Integer, PrimaryKeyConstraint, String, Text
from sqlalchemy.orm import relationship

from carina.db.base_class import Base, TimestampMixin
from carina.enums import RetailerStatuses


class Retailer(Base, TimestampMixin):
    __tablename__ = "retailer"

    id = Column(Integer, primary_key=True)
    slug = Column(String, nullable=False, index=True)
    status = Column(Enum(RetailerStatuses), nullable=False, index=True, default=RetailerStatuses.TEST)

    fetch_types = relationship("FetchType", back_populates="retailers", secondary="retailer_fetch_type")
    retailer_fetch_types = relationship("RetailerFetchType", back_populates="retailer", overlaps="fetch_types")

    rewards = relationship("Reward", back_populates="retailer")
    reward_configs = relationship("RewardConfig", back_populates="retailer")
    reward_campaigns = relationship("RewardCampaign", back_populates="retailer")

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}: ({self.id}) {self.slug}"


class FetchType(Base, TimestampMixin):
    __tablename__ = "fetch_type"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    required_fields = Column(Text, nullable=True)
    path = Column(String, nullable=False)

    retailers = relationship(
        "Retailer", back_populates="fetch_types", secondary="retailer_fetch_type", overlaps="retailer_fetch_types"
    )
    retailer_fetch_types = relationship(
        "RetailerFetchType", back_populates="fetch_type", overlaps="fetch_types,retailers"
    )
    reward_configs = relationship("RewardConfig", back_populates="fetch_type")

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}: ({self.id}) {self.name}"


class RetailerFetchType(Base, TimestampMixin):
    __tablename__ = "retailer_fetch_type"

    retailer_id = Column(Integer, ForeignKey("retailer.id", ondelete="CASCADE"), nullable=False)
    fetch_type_id = Column(Integer, ForeignKey("fetch_type.id", ondelete="CASCADE"), nullable=False)
    agent_config = Column(Text, nullable=True)

    retailer = relationship("Retailer", back_populates="retailer_fetch_types", overlaps="fetch_types,retailers")
    fetch_type = relationship("FetchType", back_populates="retailer_fetch_types", overlaps="fetch_types,retailers")
    __table_args__ = (PrimaryKeyConstraint("retailer_id", "fetch_type_id"),)

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}: {self.retailer} - {self.fetch_type}"

    def load_agent_config(self) -> dict:
        if self.agent_config in ["", None]:
            return {}

        return yaml.safe_load(self.agent_config)
