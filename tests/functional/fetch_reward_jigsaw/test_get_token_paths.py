import json

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import httpretty
import pytest
import requests

from fastapi import status
from sqlalchemy.future import select

from app.core.config import redis_raw
from app.fetch_reward.base import AgentError
from app.fetch_reward.jigsaw import Jigsaw
from app.models.reward import Reward

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture
    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session

    from app.models import RetailerFetchType, RewardConfig


@httpretty.activate
def test_jigsaw_agent_expired_token(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()

    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    now = datetime.now(tz=timezone.utc)
    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/getToken",
        body=json.dumps(
            {
                "status": 2000,
                "status_description": "OK",
                "messages": [],
                "PartnerRef": "",
                "data": {
                    "__type": "Response.getToken:#Jigsaw.API.Service",
                    "Token": test_token,
                    # jidsaw returns a naive datetime here
                    "Expires": (now.replace(tzinfo=None) - timedelta(days=1)).isoformat(),
                    "TestMode": True,
                },
            }
        ),
        status=200,
    )

    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat
    spy_logger = mocker.spy(Jigsaw, "logger")

    with pytest.raises(AgentError):
        with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
            agent.fetch_reward()

    spy_logger.exception.assert_called_once()
    assert db_session.scalar(select(Reward).where(Reward.reward_config_id == jigsaw_reward_config.id)) is None
    db_session.refresh(issuance_retry_task_no_reward)
    task_params = issuance_retry_task_no_reward.get_params()
    assert all(val not in task_params for val in ["issued_date", "expiry_date", "reward_uuid", "reward_code"])

    agent_state_params = json.loads(task_params["agent_state_params_raw"])
    assert "customer_card_ref" in agent_state_params
    assert "might_need_reversal" not in agent_state_params
    spy_redis_set.assert_not_called()


@httpretty.activate
def test_jigsaw_agent_get_token_retry_paths(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    spy_redis_set = mocker.spy(redis_raw, "set")

    for jigsaw_status, description, expected_status in (
        (5000, "Internal Server Error", status.HTTP_500_INTERNAL_SERVER_ERROR),
        (5003, "Service Unavailable", status.HTTP_503_SERVICE_UNAVAILABLE),
    ):

        httpretty.register_uri(
            "POST",
            f"{agent_config['base_url']}/order/V4/getToken",
            body=json.dumps(
                {
                    "status": jigsaw_status,
                    "status_description": description,
                    "messages": [
                        {
                            "isError": True,
                            "id": "5",
                            "Info": "RetryableError",
                        }
                    ],
                }
            ),
            status=200,
        )

        with pytest.raises(requests.RequestException) as exc_info:
            with Jigsaw(
                db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward
            ) as agent:
                agent.fetch_reward()

        assert exc_info.value.response.status_code == expected_status
        spy_redis_set.assert_not_called()

        task_params = issuance_retry_task_no_reward.get_params()
        agent_state_params = json.loads(task_params["agent_state_params_raw"])
        assert "customer_card_ref" in agent_state_params
        assert "might_need_reversal" not in agent_state_params


@httpretty.activate
def test_jigsaw_agent_get_token_failure_paths(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    spy_redis_set = mocker.spy(redis_raw, "set")

    for jigsaw_status, description, expected_status in (
        (4003, "Forbidden", status.HTTP_403_FORBIDDEN),
        (4001, "Unauthorised", status.HTTP_401_UNAUTHORIZED),
    ):

        httpretty.register_uri(
            "POST",
            f"{agent_config['base_url']}/order/V4/getToken",
            body=json.dumps(
                {
                    "status": jigsaw_status,
                    "status_description": description,
                    "messages": [
                        {
                            "isError": True,
                            "id": "5",
                            "Info": "NonRetryableError",
                        }
                    ],
                }
            ),
            status=200,
        )

        with pytest.raises(requests.RequestException) as exc_info:
            with Jigsaw(
                db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward
            ) as agent:
                agent.fetch_reward()

        assert exc_info.value.response.status_code == expected_status
        spy_redis_set.assert_not_called()

        task_params = issuance_retry_task_no_reward.get_params()
        agent_state_params = json.loads(task_params["agent_state_params_raw"])
        assert "customer_card_ref" in agent_state_params
        assert "might_need_reversal" not in agent_state_params


@httpretty.activate
def test_jigsaw_agent_get_token_unexpected_error_response(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    now = datetime.now(tz=timezone.utc)
    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/getToken",
        status=200,
        body=json.dumps(
            {
                "status": 9000,
                "status_description": "OMG",
                "messages": [
                    {
                        "isError": True,
                        "id": "9000",
                        "Info": "AHHHHHHHHHHHH!!!!",
                    }
                ],
            }
        ),
    )

    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat
    spy_logger = mocker.spy(Jigsaw, "logger")

    with pytest.raises(AgentError) as exc_info:
        with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
            agent.fetch_reward()

    spy_logger.exception.assert_called_with(
        "Exception occurred while fetching a new Jigsaw reward, exiting agent gracefully.", exc_info=exc_info.value
    )
    assert exc_info.value.args[0] == "Jigsaw: unknown error returned. status: 9000 OMG, message: 9000 AHHHHHHHHHHHH!!!!"
    assert db_session.scalar(select(Reward).where(Reward.reward_config_id == jigsaw_reward_config.id)) is None
    db_session.refresh(issuance_retry_task_no_reward)
    task_params = issuance_retry_task_no_reward.get_params()
    assert all(val not in task_params for val in ["issued_date", "expiry_date", "reward_uuid", "reward_code"])

    spy_redis_set.assert_not_called()

    agent_state_params = json.loads(task_params["agent_state_params_raw"])
    assert "customer_card_ref" in agent_state_params
    assert "might_need_reversal" not in agent_state_params
