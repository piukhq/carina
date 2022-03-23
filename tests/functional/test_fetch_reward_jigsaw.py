# pylint: disable=too-many-arguments,too-many-locals

import json

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Generator, cast
from unittest import mock
from uuid import uuid4

import httpretty
import pytest
import requests

from cryptography.fernet import Fernet
from fastapi import status
from retry_tasks_lib.db.models import TaskTypeKey, TaskTypeKeyValue
from sqlalchemy import insert
from sqlalchemy.future import select

from app.core.config import redis_raw, settings
from app.fetch_reward.base import AgentError
from app.fetch_reward.jigsaw import Jigsaw
from app.models.reward import Reward

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture
    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session

    from app.models import RetailerFetchType, RewardConfig


@pytest.fixture(scope="function", autouse=True)
def clean_redis() -> Generator:
    redis_raw.delete(Jigsaw.redis_token_key)
    yield
    redis_raw.delete(Jigsaw.redis_token_key)


@pytest.fixture(scope="module", autouse=True)
def populate_fernet_key() -> Generator:
    settings.JIGSAW_AGENT_ENCRYPTION_KEY = Fernet.generate_key().decode()
    yield


@pytest.fixture(scope="module")
def fernet(populate_fernet_key: None) -> Fernet:
    return Fernet(settings.JIGSAW_AGENT_ENCRYPTION_KEY.encode())


@httpretty.activate
def test_jigsaw_agent_ok(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: Fernet,
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


@httpretty.activate
def test_jigsaw_agent_ok_token_already_set(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: Fernet,
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

    redis_raw.set(Jigsaw.redis_token_key, fernet.encrypt(test_token.encode()), timedelta(days=1))
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


@httpretty.activate
def test_jigsaw_agent_ok_no_retry_task(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    fernet: Fernet,
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
    redis_raw.set(Jigsaw.redis_token_key, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
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
    spy_redis_set.assert_not_called()


@httpretty.activate
def test_jigsaw_agent_ok_card_ref_in_task_params(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: Fernet,
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
    redis_raw.set(Jigsaw.redis_token_key, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
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
    spy_redis_set.assert_not_called()


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
    task_params_keys = issuance_retry_task_no_reward.get_params().keys()
    assert all(val not in task_params_keys for val in ["issued_date", "expiry_date", "reward_uuid", "reward_code"])
    assert "customer_card_ref" in task_params_keys
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
                            "info": "RetryableError",
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
                            "info": "NonRetryableError",
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
                        "info": "AHHHHHHHHHHHH!!!!",
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
    task_params_keys = issuance_retry_task_no_reward.get_params().keys()
    assert all(val not in task_params_keys for val in ["issued_date", "expiry_date", "reward_uuid", "reward_code"])
    assert "customer_card_ref" in task_params_keys
    spy_redis_set.assert_not_called()


@httpretty.activate
def test_jigsaw_agent_register_retry_paths(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: Fernet,
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    now = datetime.now(tz=timezone.utc)
    redis_raw.set(Jigsaw.redis_token_key, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    for jigsaw_status, description, expected_status in (
        (5000, "Internal Server Error", status.HTTP_500_INTERNAL_SERVER_ERROR),
        (5003, "Service Unavailable", status.HTTP_503_SERVICE_UNAVAILABLE),
    ):

        httpretty.register_uri(
            "POST",
            f"{agent_config['base_url']}/order/V4/register",
            body=json.dumps(
                {
                    "status": jigsaw_status,
                    "status_description": description,
                    "messages": [
                        {
                            "isError": True,
                            "id": "5",
                            "info": "RetryableError",
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
        mock_uuid.assert_called()
        spy_redis_set.assert_not_called()
        db_session.refresh(issuance_retry_task_no_reward)
        task_params = issuance_retry_task_no_reward.get_params()
        assert all(
            val not in task_params.keys() for val in ["issued_date", "expiry_date", "reward_uuid", "reward_code"]
        )
        assert task_params["customer_card_ref"] == str(card_ref)


@httpretty.activate
def test_jigsaw_agent_register_failure_paths(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: Fernet,
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    now = datetime.now(tz=timezone.utc)
    redis_raw.set(Jigsaw.redis_token_key, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        body=json.dumps(
            {
                "status": 4001,
                "status_description": "Unauthorised",
                "messages": [
                    {
                        "isError": True,
                        "id": "30001",
                        "info": "Access denied",
                    }
                ],
            }
        ),
        status=200,
    )

    with pytest.raises(requests.RequestException) as exc_info:
        with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
            agent.fetch_reward()

    assert exc_info.value.response.status_code == status.HTTP_401_UNAUTHORIZED
    mock_uuid.assert_called()
    spy_redis_set.assert_not_called()
    db_session.refresh(issuance_retry_task_no_reward)
    task_params = issuance_retry_task_no_reward.get_params()
    assert all(val not in task_params.keys() for val in ["issued_date", "expiry_date", "reward_uuid", "reward_code"])
    assert task_params["customer_card_ref"] == str(card_ref)


@httpretty.activate
def test_jigsaw_agent_register_retry_get_token(
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    issuance_retry_task_no_reward: "RetryTask",
    fernet: Fernet,
) -> None:

    retry_error_ids = ["10003", "10006", "10007"]
    tx_value = 15
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    card_num = "NEW-REWARD-CODE"
    now = datetime.now(tz=timezone.utc)
    mock_redis = mocker.patch("app.fetch_reward.jigsaw.redis_raw")
    mock_uuid = mocker.patch("app.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("app.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat
    get_token_url = f"{agent_config['base_url']}/order/V4/getToken"
    register_url = f"{agent_config['base_url']}/order/V4/register"

    httpretty.register_uri(
        "POST",
        get_token_url,
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

    def register_response_generator(
        request: requests.Request, uri: str, response_headers: dict  # pylint: disable=unused-argument
    ) -> tuple[int, dict, str]:

        for msg_id in retry_error_ids:

            if request.headers.get("Token") == f"invalid-token-{msg_id}":
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
                                    "id": msg_id,
                                    "info": "Token invalid",
                                }
                            ],
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
        )

    httpretty.register_uri("POST", register_url, body=register_response_generator)

    def encrypted(token: str) -> bytes:
        return fernet.encrypt(token.encode())

    mock_redis.get.side_effect = [
        encrypted("invalid-token-10003"),
        None,
        encrypted("invalid-token-10006"),
        None,
        encrypted("invalid-token-10007"),
        None,
    ]
    for _ in retry_error_ids:

        with Jigsaw(db_session, jigsaw_reward_config, agent_config, retry_task=issuance_retry_task_no_reward) as agent:
            reward, issued, expiry = agent.fetch_reward()

        mock_uuid.assert_called()
        mock_redis.set.assert_called()
        mock_redis.get.assert_called()
        mock_redis.delete.assert_called()

        db_session.refresh(issuance_retry_task_no_reward)
        audit = issuance_retry_task_no_reward.audit_data
        assert audit[0]["request"]["url"] == register_url
        assert audit[0]["response"]["jigsaw_status"] == "4001 Unauthorised"
        assert audit[1]["request"]["url"] == get_token_url
        assert audit[1]["response"]["jigsaw_status"] == "2000 OK"
        assert audit[2]["request"]["url"] == register_url
        assert audit[2]["response"]["jigsaw_status"] == "2000 OK"

        assert reward is not None
        assert str(reward.id) == str(card_ref)
        assert reward.code == card_num
        assert issued == now.timestamp()
        assert expiry == (now + timedelta(days=1)).timestamp()
        issuance_retry_task_no_reward.audit_data = []
        db_session.delete(reward)
        db_session.commit()
