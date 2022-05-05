# pylint: disable=too-many-arguments,too-many-locals

import json

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, cast
from unittest import mock
from uuid import uuid4

import httpretty

from retry_tasks_lib.db.models import TaskTypeKey, TaskTypeKeyValue
from sqlalchemy import insert
from sqlalchemy.future import select

from app.core.config import redis_raw
from app.fetch_reward.jigsaw import Jigsaw

from . import AnswerBotBase

if TYPE_CHECKING:  # pragma: no cover
    from cryptography.fernet import Fernet
    from pytest_mock import MockerFixture
    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session

    from app.models import RetailerFetchType, RewardConfig


@httpretty.activate
def test_jigsaw_agent_ok(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: "Fernet",
) -> None:
    sample_url = "http://sample.url"
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    tx_value = jigsaw_reward_config.load_required_fields_values()["transaction_value"]
    card_ref = uuid4()
    card_num = "NEW-REWARD-CODE"
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
                    "Expires": (now.replace(tzinfo=None) + timedelta(days=1)).isoformat(),
                    "TestMode": True,
                },
            }
        ),
        status=200,
    )
    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        body=json.dumps(
            {
                "status": 2000,
                "status_description": "OK",
                "messages": [],
                "PartnerRef": "",
                "data": {
                    "__type": "Response_Data.cardData:#Order_V4",
                    "customer_card_ref": str(card_ref),
                    "reference": "339069",
                    "number": card_num,
                    "pin": "",
                    "transaction_value": tx_value,
                    "expiry_date": (now + timedelta(days=1)).isoformat(),
                    "balance": tx_value,
                    "voucher_url": sample_url,
                    "card_status": 1,
                },
            }
        ),
        status=200,
    )
    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
        reward, issued, expiry = agent.fetch_reward()

    assert reward is not None
    assert str(reward.id) == str(card_ref)
    assert reward.code == card_num
    assert issued == now.timestamp()
    assert expiry == (now + timedelta(days=1)).timestamp()

    mock_uuid.assert_called_once()
    spy_redis_set.assert_called_once_with(Jigsaw.REDIS_TOKEN_KEY, mock.ANY, timedelta(days=1))
    assert fernet.decrypt(cast(bytes, redis_raw.get(Jigsaw.REDIS_TOKEN_KEY))).decode() == test_token

    task_params = issuance_retry_task_no_reward.get_params()
    assert json.loads(task_params["agent_state_params_raw"]) == {"associated_url": sample_url}


@httpretty.activate
def test_jigsaw_agent_ok_token_already_set(
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
    sample_url = "http://sample.url"
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    now = datetime.now(tz=timezone.utc)
    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        body=json.dumps(
            {
                "status": 2000,
                "status_description": "OK",
                "messages": [],
                "PartnerRef": "",
                "data": {
                    "__type": "Response_Data.cardData:#Order_V4",
                    "customer_card_ref": str(card_ref),
                    "reference": "339069",
                    "number": card_num,
                    "pin": "",
                    "transaction_value": tx_value,
                    "expiry_date": (now + timedelta(days=1)).isoformat(),
                    "balance": tx_value,
                    "voucher_url": sample_url,
                    "card_status": 1,
                },
            }
        ),
        status=200,
    )

    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")

    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
        reward, issued, expiry = agent.fetch_reward()

    assert reward is not None
    assert str(reward.id) == str(card_ref)
    assert reward.code == card_num
    assert issued == now.timestamp()
    assert expiry == (now + timedelta(days=1)).timestamp()

    mock_uuid.assert_called_once()
    spy_redis_set.assert_not_called()

    task_params = issuance_retry_task_no_reward.get_params()
    assert json.loads(task_params["agent_state_params_raw"]) == {"associated_url": sample_url}


@httpretty.activate
def test_jigsaw_agent_ok_card_ref_in_task_params(
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
    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        body=json.dumps(
            {
                "status": 2000,
                "status_description": "OK",
                "messages": [],
                "PartnerRef": "",
                "data": {
                    "__type": "Response_Data.cardData:#Order_V4",
                    "customer_card_ref": str(card_ref),
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
        status=200,
    )
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    db_session.execute(
        insert(TaskTypeKeyValue).values(
            value=json.dumps({"customer_card_ref": str(card_ref)}),
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

    with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
        reward, issued, expiry = agent.fetch_reward()

    assert reward is not None
    assert str(reward.id) == str(card_ref)
    assert reward.code == card_num
    assert issued == now.timestamp()
    assert expiry == (now + timedelta(days=1)).timestamp()

    mock_uuid.assert_not_called()
    spy_redis_set.assert_not_called()

    task_params = issuance_retry_task_no_reward.get_params()
    assert "might_need_reversal" not in task_params["agent_state_params_raw"]


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
