# pylint: disable=too-many-arguments,too-many-locals

import json

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

import httpretty
import pytest

from retry_tasks_lib.db.models import TaskTypeKey, TaskTypeKeyValue
from sqlalchemy import insert
from sqlalchemy.future import select

from app.core.config import redis_raw
from app.fetch_reward.base import AgentError
from app.fetch_reward.jigsaw import Jigsaw

from . import AnswerBotBase

if TYPE_CHECKING:  # pragma: no cover
    from cryptography.fernet import Fernet
    from pytest_mock import MockerFixture
    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session

    from app.models import RetailerFetchType, RewardConfig


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_no_previous_error_ok(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: "Fernet",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    tx_value = jigsaw_reward_config.load_required_fields_values()["transaction_value"]
    card_ref = uuid4()
    card_num = "NEW-REWARD-CODE"
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    now = datetime.now(tz=timezone.utc)
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")

    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4")
    successful_card_ref = uuid4()
    mock_uuid.side_effect = [card_ref, successful_card_ref]
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    class AnswerBot(AnswerBotBase):
        def response_generator(
            self, request: httpretty.core.HTTPrettyRequest, uri: str, response_headers: dict
        ) -> tuple[int, dict, str]:

            self._update_calls_end_get_endpoint(uri)
            requests_card_ref = json.loads(request.body)["customer_card_ref"]

            if requests_card_ref == str(card_ref):
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
                            "customer_card_ref": requests_card_ref,
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

    answer_bot = AnswerBot()
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/register", body=answer_bot.response_generator)

    with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
        reward, issued, expiry = agent.fetch_reward()

    assert answer_bot.calls["register"] == 2
    assert "reversal" not in answer_bot.calls
    assert reward is not None
    assert str(reward.id) == str(card_ref)
    assert reward.code == card_num
    assert issued == now.timestamp()
    assert expiry == (now + timedelta(days=1)).timestamp()

    assert mock_uuid.call_count == 2
    spy_redis_set.assert_not_called()
    task_params = issuance_retry_task_no_reward.get_params()
    assert json.loads(task_params["agent_state_params_raw"])["customer_card_ref"] == str(successful_card_ref)


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
    mock_uuid4 = mocker.patch("app.fetch_reward.jigsaw.uuid4")
    expected_last_val = uuid4()
    mock_uuid4.side_effect = (uuid4(), uuid4(), uuid4(), expected_last_val, uuid4())

    class AnswerBot(AnswerBotBase):
        def response_generator(
            self, request: httpretty.core.HTTPrettyRequest, uri: str, response_headers: dict
        ) -> tuple[int, dict, str]:
            self._update_calls_end_get_endpoint(uri)
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

    with pytest.raises(AgentError) as exc_info:
        with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
            agent.fetch_reward()

    assert answer_bot.calls["register"] == 4
    assert "reversal" not in answer_bot.calls
    assert exc_info.value.args[0] == (
        "Jigsaw: unknown error returned. status: 4000 Validation failed, message: 40028 order already exists"
    )
    assert mock_uuid4.call_count == 4
    task_params = issuance_retry_task_no_reward.get_params()
    assert all(val not in task_params.keys() for val in ["issued_date", "expiry_date", "reward_uuid", "reward_code"])
    assert json.loads(task_params["agent_state_params_raw"])["customer_card_ref"] == str(expected_last_val)


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_previous_error_ok(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: "Fernet",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    tx_value = jigsaw_reward_config.load_required_fields_values()["transaction_value"]
    card_ref = uuid4()
    card_num = "NEW-REWARD-CODE"
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    now = datetime.now(tz=timezone.utc)
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")

    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4")
    mock_uuid.return_value = card_ref
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
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

            endpoint = self._update_calls_end_get_endpoint(uri)

            if endpoint == "register":

                if self.calls["reversal"] < 1:
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

            if endpoint == "reversal":
                return (
                    200,
                    response_headers,
                    json.dumps(
                        {
                            "status": 2000,
                            "status_description": "OK",
                            "messages": [],
                            "PartnerRef": "",
                            "data": None,
                        }
                    ),
                )

            raise ValueError("should not have got here!")

    answer_bot = AnswerBot()
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/register", body=answer_bot.response_generator)
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/reversal", body=answer_bot.response_generator)

    with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
        reward, issued, expiry = agent.fetch_reward()

    assert answer_bot.calls["register"] == 2
    assert answer_bot.calls["reversal"] == 1
    assert reward is not None
    assert str(reward.id) == str(card_ref)
    assert reward.code == card_num
    assert issued == now.timestamp()
    assert expiry == (now + timedelta(days=1)).timestamp()

    assert mock_uuid.call_count == 1
    spy_redis_set.assert_not_called()
    task_params = issuance_retry_task_no_reward.get_params()
    agent_state_params = json.loads(task_params["agent_state_params_raw"])
    assert "customer_card_ref" not in agent_state_params
    assert agent_state_params["might_need_reversal"] is True


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
    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4")
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

            match self._update_calls_end_get_endpoint(uri):
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

    with pytest.raises(AgentError):
        with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
            agent.fetch_reward()

    assert answer_bot.calls["register"] == 4
    assert answer_bot.calls["reversal"] == 3

    assert mock_uuid.call_count == 1
    spy_redis_set.assert_not_called()
    task_params = issuance_retry_task_no_reward.get_params()
    assert all(val not in task_params for val in ["issued_date", "expiry_date", "reward_uuid", "reward_code"])
    agent_state_params = json.loads(task_params["agent_state_params_raw"])
    assert agent_state_params["customer_card_ref"] == str(card_ref)
    assert agent_state_params["might_need_reversal"] is True
