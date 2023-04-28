import json

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

import httpretty
import pytest
import requests

from fastapi import status
from retry_tasks_lib.db.models import TaskTypeKey, TaskTypeKeyValue
from sqlalchemy import insert
from sqlalchemy.future import select

from carina.core.config import redis_raw
from carina.fetch_reward.base import AgentError
from carina.fetch_reward.jigsaw import Jigsaw

from . import AnswerBotBase

if TYPE_CHECKING:  # pragma: no cover
    from cryptography.fernet import Fernet
    from pytest_mock import MockerFixture
    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session

    from carina.models import RetailerFetchType, RewardConfig


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_no_previous_error_max_retries_exceeded(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: "Fernet",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(test_token.encode()), timedelta(days=1))
    mock_uuid4 = mocker.patch("carina.fetch_reward.jigsaw.uuid4")
    expected_last_val = uuid4()
    mock_uuid4.side_effect = (uuid4(), uuid4(), uuid4(), expected_last_val, uuid4())

    class AnswerBot(AnswerBotBase):
        def response_generator(
            self, request: httpretty.core.HTTPrettyRequest, uri: str, response_headers: dict
        ) -> tuple[int, dict, str]:
            self._update_calls_and_get_endpoint(uri)
            return (
                200,
                response_headers,
                json.dumps(
                    {
                        "status": 4000,
                        "status_description": "Validation failed",
                        "messages": [
                            {
                                "isError": True,
                                "id": "40028",
                                "Info": "order already exists",
                            }
                        ],
                        "PartnerRef": "",
                        "data": None,
                    }
                ),
            )

    answer_bot = AnswerBot()
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/register", body=answer_bot.response_generator)

    with pytest.raises(AgentError) as exc_info, Jigsaw(
        db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward
    ) as agent:
        agent.fetch_reward()

    assert answer_bot.calls["register"] == 4
    assert "reversal" not in answer_bot.calls
    assert exc_info.value.args[0] == (
        "Jigsaw: unknown error returned. status: 4000 Validation failed, endpoint: /order/V4/register, "
        f"message: 40028 order already exists, customer card ref: {expected_last_val}"
    )
    assert mock_uuid4.call_count == 4
    task_params = issuance_retry_task_no_reward.get_params()
    assert all(val not in task_params.keys() for val in ("issued_date", "expiry_date", "reward_uuid", "reward_code"))
    assert json.loads(task_params["agent_state_params_raw"])["customer_card_ref"] == str(expected_last_val)


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_previous_error_max_retries_exceeded(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: "Fernet",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(b"test-token"), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_uuid = mocker.patch("carina.fetch_reward.jigsaw.uuid4")
    mock_uuid.return_value = card_ref

    db_session.execute(
        insert(TaskTypeKeyValue).values(
            value=json.dumps({"might_need_reversal": True}),
            retry_task_id=issuance_retry_task_no_reward.retry_task_id,
            task_type_key_id=(
                select(TaskTypeKey.task_type_key_id)
                .where(
                    TaskTypeKey.task_type_id == issuance_retry_task_no_reward.task_type_id,
                    TaskTypeKey.name == "agent_state_params_raw",
                )
                .scalar_subquery()
            ),
        )
    )
    db_session.commit()

    class AnswerBot(AnswerBotBase):
        def response_generator(
            self, request: httpretty.core.HTTPrettyRequest, uri: str, response_headers: dict
        ) -> tuple[int, dict, str]:

            match self._update_calls_and_get_endpoint(uri):
                case "register":
                    return (
                        200,
                        response_headers,
                        json.dumps(
                            {
                                "status": 4000,
                                "status_description": "Validation failed",
                                "messages": [
                                    {
                                        "isError": True,
                                        "id": "40028",
                                        "Info": "order already exists",
                                    }
                                ],
                                "PartnerRef": "",
                                "data": None,
                            }
                        ),
                    )

                case "reversal":
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
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/register", body=answer_bot.response_generator)
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/reversal", body=answer_bot.response_generator)

    with pytest.raises(AgentError), Jigsaw(
        db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward
    ) as agent:
        agent.fetch_reward()

    assert answer_bot.calls["register"] == 4
    assert answer_bot.calls["reversal"] == 1

    assert mock_uuid.call_count == 4
    spy_redis_set.assert_not_called()
    task_params = issuance_retry_task_no_reward.get_params()
    assert all(val not in task_params for val in ("issued_date", "expiry_date", "reward_uuid", "reward_code"))
    agent_state_params = json.loads(task_params["agent_state_params_raw"])
    assert agent_state_params["customer_card_ref"] == str(card_ref)
    assert agent_state_params["might_need_reversal"] is True


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_previous_error_need_new_token(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: "Fernet",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    success_card_ref = uuid4()
    card_num = "sample-reward-code"
    tx_value = jigsaw_reward_config.load_required_fields_values()["transaction_value"]
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(b"test-token"), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_uuid = mocker.patch("carina.fetch_reward.jigsaw.uuid4")
    mock_uuid.side_effect = [card_ref, success_card_ref]
    success_token = "test-token-success"
    now = datetime.now(tz=timezone.utc)
    mock_datetime = mocker.patch("carina.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    db_session.execute(
        insert(TaskTypeKeyValue).values(
            value=json.dumps({"might_need_reversal": True}),
            retry_task_id=issuance_retry_task_no_reward.retry_task_id,
            task_type_key_id=(
                select(TaskTypeKey.task_type_key_id)
                .where(
                    TaskTypeKey.task_type_id == issuance_retry_task_no_reward.task_type_id,
                    TaskTypeKey.name == "agent_state_params_raw",
                )
                .scalar_subquery()
            ),
        )
    )
    db_session.commit()

    class AnswerBot(AnswerBotBase):
        def response_generator(
            self, request: httpretty.core.HTTPrettyRequest, uri: str, response_headers: dict
        ) -> tuple[int, dict, str]:

            match self._update_calls_and_get_endpoint(uri):
                case "register":
                    if self.calls["reversal"] < 2:
                        return (
                            200,
                            response_headers,
                            json.dumps(
                                {
                                    "status": 4000,
                                    "status_description": "Validation failed",
                                    "messages": [
                                        {
                                            "isError": True,
                                            "id": "40028",
                                            "Info": "order already exists",
                                        }
                                    ],
                                    "PartnerRef": "",
                                    "data": None,
                                }
                            ),
                        )

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
                                    "__type": "Response_Data.cardData:#Order_V4",
                                    "customer_card_ref": json.loads(request.body)["customer_card_ref"],
                                    "reference": "339069",
                                    "number": card_num,
                                    "pin": "",
                                    "transaction_value": tx_value,
                                    "expiry_date": (now + timedelta(days=1)).isoformat(),
                                    "balance": tx_value,
                                    "voucher_url": "https://sample.url",
                                    "card_status": 1,
                                },
                            }
                        ),
                    )

                case "reversal":
                    if request.headers["token"] == success_token:
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

                    return (
                        200,
                        response_headers,
                        json.dumps(
                            {
                                "status": 4001,
                                "status_description": "Unauthorised",
                                "messages": [
                                    {
                                        "isError": True,
                                        "id": "10003",
                                        "Info": "Token invalid",
                                    }
                                ],
                            }
                        ),
                    )

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
                                    "Token": success_token,
                                    # jigsaw returns a naive datetime here
                                    "Expires": (now.replace(tzinfo=None) + timedelta(days=1)).isoformat(),
                                    "TestMode": True,
                                },
                            }
                        ),
                    )

                case _:
                    raise ValueError("should not have got here!")

    answer_bot = AnswerBot()
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/register", body=answer_bot.response_generator)
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/reversal", body=answer_bot.response_generator)
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/getToken", body=answer_bot.response_generator)

    with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
        reward_data = agent.fetch_reward()

    assert answer_bot.calls["register"] == 2
    assert answer_bot.calls["reversal"] == 2
    assert answer_bot.calls["getToken"] == 1

    assert mock_uuid.call_count == 2
    spy_redis_set.assert_called_once()

    assert reward_data.reward is not None
    assert str(reward_data.reward.id) == str(success_card_ref)
    assert reward_data.reward.code == card_num
    assert reward_data.issued_date == now.timestamp()
    assert reward_data.expiry_date == (now + timedelta(days=1)).timestamp()
    assert reward_data.validity_days is None

    task_params = issuance_retry_task_no_reward.get_params()
    assert all(val not in task_params for val in ("issued_date", "expiry_date", "reward_uuid", "reward_code"))
    agent_state_params = json.loads(task_params["agent_state_params_raw"])

    assert agent_state_params["customer_card_ref"] == str(success_card_ref)
    assert agent_state_params["reversal_customer_card_ref"] == str(card_ref)
    assert agent_state_params["might_need_reversal"] is False


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_previous_error_retry_paths(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: "Fernet",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(b"test-token"), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    card_ref = uuid4()
    mock_uuid = mocker.patch("carina.fetch_reward.jigsaw.uuid4")
    mock_uuid.return_value = card_ref

    db_session.execute(
        insert(TaskTypeKeyValue).values(
            value=json.dumps({"might_need_reversal": True}),
            retry_task_id=issuance_retry_task_no_reward.retry_task_id,
            task_type_key_id=(
                select(TaskTypeKey.task_type_key_id)
                .where(
                    TaskTypeKey.task_type_id == issuance_retry_task_no_reward.task_type_id,
                    TaskTypeKey.name == "agent_state_params_raw",
                )
                .scalar_subquery()
            ),
        )
    )
    db_session.commit()

    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        body=json.dumps(
            {
                "status": 4000,
                "status_description": "Validation failed",
                "messages": [
                    {
                        "isError": True,
                        "id": "40028",
                        "Info": "order already exists",
                    }
                ],
                "PartnerRef": "",
                "data": None,
            }
        ),
        status=200,
    )
    expected_call_count = 2
    for jigsaw_status, description, expected_status in (
        (5000, "Internal Server Error", status.HTTP_500_INTERNAL_SERVER_ERROR),
        (5003, "Service Unavailable", status.HTTP_503_SERVICE_UNAVAILABLE),
    ):
        httpretty.register_uri(
            "POST",
            f"{agent_config['base_url']}/order/V4/reversal",
            body=json.dumps(
                {
                    "status": jigsaw_status,
                    "status_description": description,
                    "messages": [
                        {
                            "isError": True,
                            "id": "30001",
                            "Info": "Access denied",
                        }
                    ],
                }
            ),
            status=200,
        )

        with pytest.raises(requests.RequestException) as exc_info, Jigsaw(
            db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward
        ) as agent:
            agent.fetch_reward()

        assert exc_info.value.response.status_code == expected_status

        assert mock_uuid.call_count == expected_call_count
        expected_call_count += 1
        spy_redis_set.assert_not_called()
        task_params = issuance_retry_task_no_reward.get_params()
        assert all(val not in task_params for val in ("issued_date", "expiry_date", "reward_uuid", "reward_code"))
        agent_state_params = json.loads(task_params["agent_state_params_raw"])
        assert agent_state_params["customer_card_ref"] == str(card_ref)
        assert agent_state_params["might_need_reversal"] is True


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_previous_error_failure_paths(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: "Fernet",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(b"test-token"), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    card_ref = uuid4()
    mock_uuid = mocker.patch("carina.fetch_reward.jigsaw.uuid4")
    mock_uuid.return_value = card_ref

    db_session.execute(
        insert(TaskTypeKeyValue).values(
            value=json.dumps({"might_need_reversal": True}),
            retry_task_id=issuance_retry_task_no_reward.retry_task_id,
            task_type_key_id=(
                select(TaskTypeKey.task_type_key_id)
                .where(
                    TaskTypeKey.task_type_id == issuance_retry_task_no_reward.task_type_id,
                    TaskTypeKey.name == "agent_state_params_raw",
                )
                .scalar_subquery()
            ),
        )
    )
    db_session.commit()

    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        body=json.dumps(
            {
                "status": 4000,
                "status_description": "Validation failed",
                "messages": [
                    {
                        "isError": True,
                        "id": "40028",
                        "Info": "order already exists",
                    }
                ],
                "PartnerRef": "",
                "data": None,
            }
        ),
        status=200,
    )

    expected_call_count = 2
    for jigsaw_status, description, expected_status in (
        (4003, "Forbidden", status.HTTP_403_FORBIDDEN),
        (4001, "Unauthorised", status.HTTP_401_UNAUTHORIZED),
    ):
        httpretty.register_uri(
            "POST",
            f"{agent_config['base_url']}/order/V4/reversal",
            body=json.dumps(
                {
                    "status": jigsaw_status,
                    "status_description": description,
                    "messages": [
                        {
                            "isError": True,
                            "id": "30001",
                            "Info": "Access denied",
                        }
                    ],
                }
            ),
            status=200,
        )

        with pytest.raises(requests.RequestException) as exc_info, Jigsaw(
            db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward
        ) as agent:
            agent.fetch_reward()

        assert exc_info.value.response.status_code == expected_status

        assert mock_uuid.call_count == expected_call_count
        expected_call_count += 1
        spy_redis_set.assert_not_called()
        task_params = issuance_retry_task_no_reward.get_params()
        assert all(val not in task_params for val in ("issued_date", "expiry_date", "reward_uuid", "reward_code"))
        agent_state_params = json.loads(task_params["agent_state_params_raw"])
        assert agent_state_params["customer_card_ref"] == str(card_ref)
        assert agent_state_params["might_need_reversal"] is True
