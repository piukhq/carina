import pytest

from carina.enums import RewardCampaignStatuses
from carina.schemas import RewardCampaignSchema


def test_reward_campaign_schema_raises() -> None:
    with pytest.raises(ValueError):
        RewardCampaignSchema(campaign_slug="   ", status=RewardCampaignStatuses.ACTIVE)
