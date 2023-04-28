import json

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

import httpretty
import pytest

from requests import HTTPError
from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKeyValue

from carina.fetch_reward import cleanup_reward
from carina.fetch_reward.jigsaw import Jigsaw
from carina.models.reward import Reward

from . import AnswerBotBase

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture
    from sqlalchemy.orm import Session

    from carina.models import RetailerFetchType, RewardConfig


@pytest.fixture
def jigsaw_reward(db_session: "Session", reward: Reward, jigsaw_reward_config: "RewardConfig") -> Reward:
    reward.reward_config_id = jigsaw_reward_config.id
    db_session.commit()
    return reward


@pytest.fixture
def jigsaw_issuance_retry_task_no_reward(
    db_session: "Session", reward_issuance_task_type: "TaskType", jigsaw_reward_config: "RewardConfig"
) -> RetryTask:
    task = RetryTask(task_type_id=reward_issuance_task_type.task_type_id)
    db_session.add(task)
    db_session.flush()

    key_ids = reward_issuance_task_type.get_key_ids_by_name()
    db_session.add_all(
        [
            TaskTypeKeyValue(
                task_type_key_id=key_ids[key],
                value=value,
                retry_task_id=task.retry_task_id,
            )
            for key, value in {
                "account_url": "http://test.url/",
                "reward_config_id": str(jigsaw_reward_config.id),
                "reward_slug": jigsaw_reward_config.reward_slug,
                "idempotency_token": str(uuid4()),
                "retailer_slug": "test-retailer",
                "campaign_slug": "test-campaign",
            }.items()
        ]
    )
    db_session.commit()
    return task


@pytest.fixture
def jigsaw_issuance_retry_task(
    db_session: "Session", jigsaw_reward: Reward, reward_issuance_task_type: "TaskType"
) -> RetryTask:
    task = RetryTask(task_type_id=reward_issuance_task_type.task_type_id)
    db_session.add(task)
    db_session.flush()

    key_ids = reward_issuance_task_type.get_key_ids_by_name()
    db_session.add_all(
        [
            TaskTypeKeyValue(
                task_type_key_id=key_ids[key],
                value=value,
                retry_task_id=task.retry_task_id,
            )
            for key, value in {
                "account_url": "http://test.url/",
                "reward_uuid": str(jigsaw_reward.id),
                "code": jigsaw_reward.code,
                "reward_config_id": str(jigsaw_reward.reward_config_id),
                "reward_slug": jigsaw_reward.reward_config.reward_slug,
                "idempotency_token": str(uuid4()),
                "retailer_slug": "test-retailer",
                "campaign_slug": "test-campaign",
            }.items()
        ]
    )
    db_session.commit()
    return task


@httpretty.activate
def test_cleanup_reward_ok(
    db_session: "Session",
    jigsaw_reward: Reward,
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_issuance_retry_task: "RetryTask",
) -> None:
    test_token = "test-token"
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()

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
                    # jigsaw returns a naive datetime here
                    "Expires": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                    "TestMode": True,
                },
            }
        ),
        status=200,
    )

    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/reversal",
        body=json.dumps(
            {
                "status": 2000,
                "status_description": "Success OK",
                "messages": [],
                "PartnerRef": "",
                "data": None,
            }
        ),
        status=200,
    )

    pre_task_params = jigsaw_issuance_retry_task.get_params()
    jigsaw_reward.allocated = True
    db_session.commit()
    assert jigsaw_reward.deleted is False
    assert pre_task_params["reward_uuid"] == str(jigsaw_reward.id)
    assert all(key in pre_task_params for key in ("reward_uuid", "code"))

    cleanup_reward(db_session, jigsaw_reward_config, jigsaw_issuance_retry_task)

    db_session.refresh(jigsaw_issuance_retry_task)
    db_session.refresh(jigsaw_reward)
    post_task_params = jigsaw_issuance_retry_task.get_params()
    assert jigsaw_reward.allocated
    assert jigsaw_reward.deleted is True
    assert all(key not in post_task_params for key in ("reward_uuid", "code", "issued_date", "expiry_date"))


def test_cleanup_reward_no_reward_uuid_in_task(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_issuance_retry_task_no_reward: "RetryTask",
) -> None:
    mock_update_reward = mocker.patch.object(Jigsaw, "update_reward_and_remove_references_from_task")
    mock_send_reversal = mocker.patch.object(Jigsaw, "_send_reversal_request")
    pre_task_params = jigsaw_issuance_retry_task_no_reward.get_params()
    assert all(key not in pre_task_params for key in ("reward_uuid", "code"))

    cleanup_reward(db_session, jigsaw_reward_config, jigsaw_issuance_retry_task_no_reward)

    db_session.refresh(jigsaw_issuance_retry_task_no_reward)

    post_task_params = jigsaw_issuance_retry_task_no_reward.get_params()
    assert all(key not in post_task_params for key in ("reward_uuid", "code", "issued_date", "expiry_date"))
    mock_update_reward.assert_not_called()
    mock_send_reversal.assert_not_called()


@httpretty.activate
def test_cleanup_reward_unexpected_error_can_be_retried(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward: Reward,
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_issuance_retry_task: "RetryTask",
) -> None:
    spy_update_reward = mocker.spy(Jigsaw, "update_reward_and_remove_references_from_task")
    spy_send_reversal = mocker.spy(Jigsaw, "_send_reversal_request")

    test_token = "test-token"
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()

    class AnswerBot(AnswerBotBase):
        def response_generator(
            self, request: httpretty.core.HTTPrettyRequest, uri: str, response_headers: dict
        ) -> tuple[int, dict, str]:
            match self._update_calls_and_get_endpoint(uri):
                case "getToken":
                    return (
                        200,
                        response_headers,
                        json.dumps(
                            {
                                "status": 2000,
                                "status_description": "OK",
                                "messages": [],
                                "PartnerRef": "",
                                "data": {
                                    "__type": "Response.getToken:#Jigsaw.API.Service",
                                    "Token": test_token,
                                    # jigsaw returns a naive datetime here
                                    "Expires": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                                    "TestMode": True,
                                },
                            }
                        ),
                    )

                case "reversal":

                    if self.calls["reversal"] < 2:
                        return (500, response_headers, "Unexpected Error")

                    return (
                        200,
                        response_headers,
                        json.dumps(
                            {
                                "status": 2000,
                                "status_description": "Success OK",
                                "messages": [],
                                "PartnerRef": "",
                                "data": None,
                            }
                        ),
                    )

                case _:
                    raise ValueError("should not have got here!")

    answer_bot = AnswerBot()
    # http://test.url/order/V4/getToken'
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/getToken", body=answer_bot.response_generator)
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/reversal", body=answer_bot.response_generator)

    pre_task_params = jigsaw_issuance_retry_task.get_params()
    jigsaw_reward.allocated = True
    db_session.commit()
    assert jigsaw_reward.deleted is False
    assert pre_task_params["reward_uuid"] == str(jigsaw_reward.id)
    assert all(key in pre_task_params for key in ("reward_uuid", "code"))

    with pytest.raises(HTTPError):
        cleanup_reward(db_session, jigsaw_reward_config, jigsaw_issuance_retry_task)

    assert answer_bot.calls["getToken"] == 1
    assert answer_bot.calls["reversal"] == 1
    spy_update_reward.assert_called_once()
    spy_send_reversal.assert_called_once()
    db_session.refresh(jigsaw_issuance_retry_task)
    db_session.refresh(jigsaw_reward)
    post_task_params = jigsaw_issuance_retry_task.get_params()
    assert jigsaw_reward.allocated
    assert jigsaw_reward.deleted is True
    assert all(key not in post_task_params for key in ("reward_uuid", "code", "issued_date", "expiry_date"))

    cleanup_reward(db_session, jigsaw_reward_config, jigsaw_issuance_retry_task)
    assert answer_bot.calls["getToken"] == 1
    assert answer_bot.calls["reversal"] == 2
    spy_update_reward.assert_called_once()
    assert spy_send_reversal.call_count == 2
