import json

from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from retry_tasks_lib.db.models import RetryTask

from carina.core.config import settings
from carina.fetch_reward import cleanup_reward, get_allocable_reward
from carina.fetch_reward.base import BaseAgent, RewardData

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture

    from carina.models import RetailerFetchType
    from tests.conftest import SetupType


def test_get_allocable_reward_ok(
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
        get_allocable_reward(db_session, reward_config, Mock(spec=RetryTask))

    spy_logger.warning.assert_called_once()


def test_cleanup_reward_ok(
    setup: "SetupType",
    pre_loaded_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task: "RetryTask",
) -> None:
    db_session, reward_config, reward = setup
    pre_task_params = issuance_retry_task.get_params()
    reward.allocated = True
    db_session.commit()
    assert reward.deleted is False
    assert pre_task_params["reward_uuid"] == str(reward.id)
    assert all(key in pre_task_params for key in ("reward_uuid", "code"))

    cleanup_reward(db_session, reward_config, issuance_retry_task)

    db_session.refresh(issuance_retry_task)
    db_session.refresh(reward)
    post_task_params = issuance_retry_task.get_params()
    assert not reward.allocated
    assert reward.deleted is False
    assert all(key not in post_task_params for key in ("reward_uuid", "code", "issued_date", "expiry_date"))


def test_cleanup_reward_reward_not_allocated(
    setup: "SetupType",
    pre_loaded_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task: "RetryTask",
) -> None:
    db_session, reward_config, reward = setup
    pre_task_params = issuance_retry_task.get_params()
    assert reward.allocated is False
    assert reward.deleted is False
    assert pre_task_params["reward_uuid"] == str(reward.id)
    assert all(key in pre_task_params for key in ("reward_uuid", "code"))

    cleanup_reward(db_session, reward_config, issuance_retry_task)

    db_session.refresh(issuance_retry_task)
    db_session.refresh(reward)
    post_task_params = issuance_retry_task.get_params()
    assert reward.allocated is False
    assert reward.deleted is False
    assert all(key not in post_task_params for key in ("reward_uuid", "code", "issued_date", "expiry_date"))


def test_cleanup_reward_reward_no_reward_uuid_in_task(
    setup: "SetupType",
    pre_loaded_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
) -> None:
    db_session, reward_config, _ = setup
    pre_task_params = issuance_retry_task_no_reward.get_params()
    assert all(key not in pre_task_params for key in ("reward_uuid", "code"))

    cleanup_reward(db_session, reward_config, issuance_retry_task_no_reward)

    db_session.refresh(issuance_retry_task_no_reward)
    post_task_params = issuance_retry_task_no_reward.get_params()
    assert all(key not in post_task_params for key in ("reward_uuid", "code", "issued_date", "expiry_date"))
