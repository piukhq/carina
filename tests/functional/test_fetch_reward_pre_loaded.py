import json

from typing import TYPE_CHECKING

import pytest

from app.core.config import settings
from app.fetch_reward import get_allocable_reward
from app.fetch_reward.base import BaseAgent, RewardData

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture
    from retry_tasks_lib.db.models import RetryTask

    from app.models import RetailerFetchType
    from tests.conftest import SetupType


def test_get_allocable_reward_ok(
    mocker: "MockerFixture",
    setup: "SetupType",
    pre_loaded_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
) -> None:
    db_session, reward_config, reward = setup
    expected_validity_days = reward_config.load_required_fields_values()["validity_days"]
    expected_result = RewardData(reward, None, None, expected_validity_days)

    reward_data = get_allocable_reward(db_session, reward_config, issuance_retry_task_no_reward)

    assert reward_data == expected_result
    db_session.refresh(issuance_retry_task_no_reward)
    agent_params = json.loads(issuance_retry_task_no_reward.get_params().get("agent_state_params_raw", "{}"))

    assert (
        agent_params.get("associated_url")
        == f"{settings.PRE_LOADED_REWARD_BASE_URL}/reward?retailer={reward.retailer.slug}&reward={reward.id}"
    )


def test_get_allocable_reward_wrong_path(mocker: "MockerFixture", setup: "SetupType") -> None:
    db_session, reward_config, _ = setup

    reward_config.fetch_type.path = "wrong.Path"
    db_session.commit()

    spy_logger = mocker.spy(BaseAgent, "logger")

    with pytest.raises(ModuleNotFoundError):
        get_allocable_reward(db_session, reward_config)

    spy_logger.warning.assert_called_once()
