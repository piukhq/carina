import json

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from unittest import mock
from uuid import uuid4

import httpretty
import pytest
import requests

from retry_tasks_lib.db.models import TaskTypeKey, TaskTypeKeyValue
from sqlalchemy import insert
from sqlalchemy.future import select

from app.fetch_reward import get_allocable_reward
from app.fetch_reward.base import BaseAgent
from app.fetch_reward.jigsaw import Jigsaw
from app.models.reward import Reward

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture
    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session

    from app.models import RetailerFetchType, RewardConfig
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


@httpretty.activate
def test_jigsaw_agent_ok(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
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
        f"{agent_config['base_url']}/order/v4/register",
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
    mock_redis = mocker.patch("app.fetch_reward.jigsaw.redis")
    mock_redis.get.return_value = None
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
    mock_redis.set.assert_called_once_with(Jigsaw.redis_token_key, test_token, 86400)


@httpretty.activate
def test_jigsaw_agent_ok_token_already_set(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
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
        f"{agent_config['base_url']}/order/v4/register",
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
    mock_redis = mocker.patch("app.fetch_reward.jigsaw.redis")
    mock_redis.get.return_value = test_token
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
    mock_redis.set.assert_not_called()


@httpretty.activate
def test_jigsaw_agent_ok_no_retry_task(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
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
        f"{agent_config['base_url']}/order/v4/register",
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
    mock_redis = mocker.patch("app.fetch_reward.jigsaw.redis")
    mock_redis.get.return_value = test_token
    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    with Jigsaw(db_session, jigsaw_reward_config, agent_config) as agent:
        reward, issued, expiry = agent.fetch_reward()

    assert reward is not None
    assert str(reward.id) == str(card_ref)
    assert reward.code == card_num
    assert issued == now.timestamp()
    assert expiry == (now + timedelta(days=1)).timestamp()

    mock_uuid.assert_called_once()
    mock_redis.set.assert_not_called()


@httpretty.activate
def test_jigsaw_agent_ok_card_ref_in_task_params(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
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
        f"{agent_config['base_url']}/order/v4/register",
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
    mock_redis = mocker.patch("app.fetch_reward.jigsaw.redis")
    mock_redis.get.return_value = test_token
    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    db_session.execute(
        insert(TaskTypeKeyValue).values(
            value=str(card_ref),
            retry_task_id=issuance_retry_task_no_reward.retry_task_id,
            task_type_key_id=(
                select(TaskTypeKey.task_type_key_id)
                .where(
                    TaskTypeKey.task_type_id == issuance_retry_task_no_reward.task_type_id,
                    TaskTypeKey.name == "customer_card_ref",
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
    mock_redis.set.assert_not_called()


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

    mock_redis = mocker.patch("app.fetch_reward.jigsaw.redis")
    mock_redis.get.return_value = None
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat
    spy_logger = mocker.spy(Jigsaw, "logger")

    with pytest.raises(ValueError):
        with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
            agent.fetch_reward()

    spy_logger.exception.assert_called_once()
    assert db_session.scalar(select(Reward).where(Reward.reward_config_id == jigsaw_reward_config.id)) is None
    db_session.refresh(issuance_retry_task_no_reward)
    assert all(
        val not in issuance_retry_task_no_reward.get_params().keys()
        for val in ["customer_card_ref", "issued_date", "expiry_date", "reward_uuid", "reward_code"]
    )
    mock_redis.set.assert_not_called()


@httpretty.activate
def test_jigsaw_agent_getToken_error_response(
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
        status=500,
    )

    mock_redis = mocker.patch("app.fetch_reward.jigsaw.redis")
    mock_redis.get.return_value = None
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat
    spy_logger = mocker.spy(Jigsaw, "logger")

    with pytest.raises(requests.RequestException):
        with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
            agent.fetch_reward()

    spy_logger.exception.assert_called_once()
    assert db_session.scalar(select(Reward).where(Reward.reward_config_id == jigsaw_reward_config.id)) is None
    db_session.refresh(issuance_retry_task_no_reward)
    assert all(
        val not in issuance_retry_task_no_reward.get_params().keys()
        for val in ["customer_card_ref", "issued_date", "expiry_date", "reward_uuid", "reward_code"]
    )
    mock_redis.set.assert_not_called()


@httpretty.activate
def test_jigsaw_agent_register_error_response(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    now = datetime.now(tz=timezone.utc)
    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/v4/register",
        body=json.dumps(
            {
                "status": 5000,
            }
        ),
        status=200,
    )
    mock_redis = mocker.patch("app.fetch_reward.jigsaw.redis")
    mock_redis.get.return_value = test_token
    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    with pytest.raises(requests.RequestException):
        with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
            agent.fetch_reward()

    mock_uuid.assert_called_once()
    mock_redis.set.assert_not_called()
    db_session.refresh(issuance_retry_task_no_reward)
    task_params = issuance_retry_task_no_reward.get_params()
    assert all(val not in task_params.keys() for val in ["issued_date", "expiry_date", "reward_uuid", "reward_code"])
    assert "customer_card_ref" in task_params
    assert task_params["customer_card_ref"] == str(card_ref)


@httpretty.activate
def test_jigsaw_agent_register_error_response_card_ref_in_task_params(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    now = datetime.now(tz=timezone.utc)
    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/v4/register",
        status=500,
    )
    mock_redis = mocker.patch("app.fetch_reward.jigsaw.redis")
    mock_redis.get.return_value = test_token
    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    db_session.execute(
        insert(TaskTypeKeyValue).values(
            value=str(card_ref),
            retry_task_id=issuance_retry_task_no_reward.retry_task_id,
            task_type_key_id=(
                select(TaskTypeKey.task_type_key_id)
                .where(
                    TaskTypeKey.task_type_id == issuance_retry_task_no_reward.task_type_id,
                    TaskTypeKey.name == "customer_card_ref",
                )
                .scalar_subquery()
            ),
        )
    )
    db_session.commit()

    with pytest.raises(requests.RequestException):
        with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
            agent.fetch_reward()

    mock_uuid.assert_not_called()
    mock_redis.set.assert_not_called()
    db_session.refresh(issuance_retry_task_no_reward)
    task_params = issuance_retry_task_no_reward.get_params()
    assert all(val not in task_params.keys() for val in ["issued_date", "expiry_date", "reward_uuid", "reward_code"])
    assert "customer_card_ref" in task_params
    assert task_params["customer_card_ref"] == str(card_ref)
