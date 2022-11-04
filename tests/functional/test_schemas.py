import pytest

from carina.enums import RewardCampaignStatuses
from carina.schemas import RewardCampaignSchema


def test_reward_campaign_schema() -> None:
    reward_campaign_payload = RewardCampaignSchema(
        campaign_slug="  Campaign-Slug  ", status=RewardCampaignStatuses.ACTIVE
    )
    assert reward_campaign_payload.campaign_slug == "campaign-slug"


def test_reward_campaign_schema_raises() -> None:
    with pytest.raises(ValueError):
        RewardCampaignSchema(campaign_slug="   ", status=RewardCampaignStatuses.ACTIVE)
