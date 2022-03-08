from typing import TYPE_CHECKING
from unittest import mock

import pytest

from app.fetch_reward import get_allocable_reward
from app.fetch_reward.base import BaseAgent

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture

    from app.models import RetailerFetchType
    from tests.conftest import SetupType


def test_get_allocable_reward_ok(
    mocker: "MockerFixture", setup: "SetupType", pre_loaded_retailer_fetch_type: "RetailerFetchType"
) -> None:
    db_session, reward_config, reward = setup
    expected_result = (reward, 10, 20)
    mock_agent_instance = mock.MagicMock(fetch_reward=lambda: expected_result)
    mock_agent_class = mocker.patch(pre_loaded_retailer_fetch_type.fetch_type.path)
    mock_agent_class.return_value.__enter__.return_value = mock_agent_instance

    reward, issued, expiry = get_allocable_reward(db_session, reward_config)

    assert (reward, issued, expiry) == expected_result


def test_get_allocable_reward_wrong_path(mocker: "MockerFixture", setup: "SetupType") -> None:
    db_session, reward_config, _ = setup

    reward_config.fetch_type.path = "wrong.Path"
    db_session.commit()

    spy_logger = mocker.spy(BaseAgent, "logger")

    with pytest.raises(ModuleNotFoundError):
        get_allocable_reward(db_session, reward_config)

    spy_logger.warning.assert_called_once()
