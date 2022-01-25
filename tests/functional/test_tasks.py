import json

from datetime import datetime
from typing import Callable
from unittest import mock

import httpretty
import pytest
import requests

from pytest_mock import MockerFixture
from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import IncorrectRetryTaskStatusError
from sqlalchemy.future import select
from sqlalchemy.orm import Session
from testfixtures import LogCapture

from app.core.config import settings
from app.enums import RewardTypeStatuses
from app.models import Reward, RewardConfig
from app.tasks.issuance import _process_issuance, issue_reward
from app.tasks.reward_cancellation import cancel_rewards
from app.tasks.reward_deletion import delete_unallocated_rewards
from app.tasks.status_adjustment import _process_status_adjustment, status_adjustment

fake_now = datetime.utcnow()


@httpretty.activate
@mock.patch("app.tasks.issuance.datetime")
def test__process_issuance_ok(
    mock_datetime: mock.Mock, reward_issuance_task_params: dict, issuance_expected_payload: dict
) -> None:

    mock_datetime.utcnow.return_value = fake_now
    httpretty.register_uri("POST", reward_issuance_task_params["account_url"], body="OK", status=200)

    response_audit = _process_issuance(reward_issuance_task_params)

    last_request = httpretty.last_request()
    assert last_request.method == "POST"
    assert last_request.url == reward_issuance_task_params["account_url"]
    assert json.loads(last_request.body) == issuance_expected_payload
    assert response_audit == {
        "timestamp": fake_now.isoformat(),
        "response": {
            "status": 200,
            "body": "OK",
        },
    }


@httpretty.activate
def test__process_issuance_http_errors(reward_issuance_task_params: dict, issuance_expected_payload: dict) -> None:

    for status, body in [
        (401, "Unauthorized"),
        (500, "Internal Server Error"),
    ]:
        httpretty.register_uri("POST", reward_issuance_task_params["account_url"], body=body, status=status)

        with pytest.raises(requests.RequestException) as excinfo:
            _process_issuance(reward_issuance_task_params)

        assert isinstance(excinfo.value, requests.RequestException)
        assert excinfo.value.response.status_code == status

        last_request = httpretty.last_request()
        assert last_request.method == "POST"
        assert json.loads(last_request.body) == issuance_expected_payload


@mock.patch("app.tasks.issuance.send_request_with_metrics")
def test__process_issuance_connection_error(
    mock_send_request_with_metrics: mock.MagicMock, reward_issuance_task_params: dict
) -> None:

    mock_send_request_with_metrics.side_effect = requests.Timeout("Request timed out")

    with pytest.raises(requests.RequestException) as excinfo:
        _process_issuance(reward_issuance_task_params)

    assert isinstance(excinfo.value, requests.Timeout)
    assert excinfo.value.response is None


@httpretty.activate
def test_reward_issuance(db_session: "Session", issuance_retry_task: RetryTask) -> None:

    httpretty.register_uri("POST", issuance_retry_task.get_params()["account_url"], body="OK", status=200)

    issue_reward(issuance_retry_task.retry_task_id)

    db_session.refresh(issuance_retry_task)

    assert issuance_retry_task.attempts == 1
    assert issuance_retry_task.next_attempt_time is None
    assert issuance_retry_task.status == RetryTaskStatuses.SUCCESS


def test_reward_issuance_wrong_status(db_session: "Session", issuance_retry_task: RetryTask) -> None:
    issuance_retry_task.status = RetryTaskStatuses.FAILED
    db_session.commit()

    with pytest.raises(IncorrectRetryTaskStatusError):
        issue_reward(issuance_retry_task.retry_task_id)

    db_session.refresh(issuance_retry_task)

    assert issuance_retry_task.attempts == 0
    assert issuance_retry_task.next_attempt_time is None
    assert issuance_retry_task.status == RetryTaskStatuses.FAILED


@httpretty.activate
def test_reward_issuance_campaign_is_cancelled(
    db_session: "Session", issuance_retry_task: RetryTask, reward_config: RewardConfig, mocker: MockerFixture
) -> None:
    """
    Test that, if the campaign has been cancelled by the time we get to issue a reward, the issuance is also cancelled
    """
    reward_config.status = RewardTypeStatuses.CANCELLED
    db_session.commit()
    import app.tasks.issuance as tasks_allocation

    spy = mocker.spy(tasks_allocation, "_process_issuance")

    issue_reward(issuance_retry_task.retry_task_id)

    db_session.refresh(issuance_retry_task)

    assert issuance_retry_task.attempts == 1
    assert issuance_retry_task.next_attempt_time is None
    assert issuance_retry_task.status == RetryTaskStatuses.CANCELLED
    assert spy.call_count == 0
    reward = db_session.execute(
        select(Reward).where(Reward.id == issuance_retry_task.get_params()["reward_uuid"])
    ).scalar_one()
    db_session.refresh(reward)
    assert not reward.allocated
    assert reward.deleted


@httpretty.activate
def test_reward_issuance_no_reward_campaign_is_cancelled(
    db_session: "Session",
    issuance_retry_task_no_reward: RetryTask,
    reward_config: RewardConfig,
    mocker: MockerFixture,
) -> None:
    """
    Test that, if the campaign has been cancelled by the time we get to issue a reward, the issuance is also cancelled.
    This is the test for when the task exists but a reward has not yet become available
    """
    reward_config.status = RewardTypeStatuses.CANCELLED
    db_session.commit()
    import app.tasks.issuance as tasks_allocation

    spy = mocker.spy(tasks_allocation, "_process_issuance")

    issue_reward(issuance_retry_task_no_reward.retry_task_id)

    db_session.refresh(issuance_retry_task_no_reward)

    assert issuance_retry_task_no_reward.attempts == 1
    assert issuance_retry_task_no_reward.next_attempt_time is None
    assert issuance_retry_task_no_reward.status == RetryTaskStatuses.CANCELLED
    assert spy.call_count == 0


@httpretty.activate
def test_reward_issuance_no_reward_but_one_available(
    db_session: "Session", issuance_retry_task_no_reward: RetryTask, mocker: MockerFixture, reward: Reward
) -> None:
    """test that an allocable reward (the pytest 'reward' fixture) is allocated, resulting in success"""
    mock_queue = mocker.patch("app.tasks.issuance.enqueue_retry_task_delay")

    httpretty.register_uri("POST", issuance_retry_task_no_reward.get_params()["account_url"], body="OK", status=200)

    issue_reward(issuance_retry_task_no_reward.retry_task_id)

    db_session.refresh(issuance_retry_task_no_reward)

    assert not mock_queue.return_value.enqueue_at.called
    assert issuance_retry_task_no_reward.attempts == 1
    assert issuance_retry_task_no_reward.next_attempt_time is None
    assert issuance_retry_task_no_reward.status == RetryTaskStatuses.SUCCESS


@httpretty.activate
def test_reward_issuance_no_reward_and_allocation_is_requeued(
    db_session: "Session",
    issuance_retry_task_no_reward: RetryTask,
    capture: LogCapture,
    mocker: MockerFixture,
    create_reward: Callable,
) -> None:
    """test that no allocable reward results in the allocation being requeued"""
    mock_queue = mocker.patch("app.tasks.issuance.enqueue_retry_task_delay")
    mock_queue.return_value = fake_now
    from app.tasks.issuance import sentry_sdk as mock_sentry_sdk

    sentry_spy = mocker.spy(mock_sentry_sdk, "capture_message")

    httpretty.register_uri("POST", issuance_retry_task_no_reward.get_params()["account_url"], body="OK", status=200)

    issue_reward(issuance_retry_task_no_reward.retry_task_id)

    db_session.refresh(issuance_retry_task_no_reward)
    mock_queue.assert_called_once()
    sentry_spy.assert_called_once()
    assert issuance_retry_task_no_reward.attempts == 1
    assert issuance_retry_task_no_reward.next_attempt_time is not None
    assert issuance_retry_task_no_reward.status == RetryTaskStatuses.WAITING
    assert any("Next attempt time at" in record.msg for record in capture.records)

    # Add new reward and check that it's allocated and marked as allocated
    reward = create_reward()  # The defaults will be correct for this test

    # call issue_reward again
    issue_reward(issuance_retry_task_no_reward.retry_task_id)

    db_session.refresh(issuance_retry_task_no_reward)
    db_session.refresh(reward)
    mock_queue.assert_called_once()  # should not have been called again
    assert issuance_retry_task_no_reward.attempts == 2
    assert issuance_retry_task_no_reward.next_attempt_time is None
    assert issuance_retry_task_no_reward.status == RetryTaskStatuses.SUCCESS
    assert issuance_retry_task_no_reward.get_params()["reward_uuid"] == str(reward.id)
    assert reward.allocated


@httpretty.activate
@mock.patch("app.tasks.status_adjustment.datetime")
def test__process_status_adjustment_ok(
    mock_datetime: mock.Mock,
    reward_status_adjustment_retry_task: RetryTask,
    adjustment_expected_payload: dict,
    adjustment_url: str,
) -> None:

    mock_datetime.utcnow.return_value = fake_now
    mock_datetime.fromisoformat = datetime.fromisoformat

    httpretty.register_uri("PATCH", adjustment_url, body="OK", status=200)

    response_audit = _process_status_adjustment(reward_status_adjustment_retry_task.get_params())

    last_request = httpretty.last_request()
    assert last_request.method == "PATCH"
    assert json.loads(last_request.body) == adjustment_expected_payload
    assert response_audit == {
        "timestamp": fake_now.isoformat(),
        "response": {
            "status": 200,
            "body": "OK",
        },
    }


@httpretty.activate
def test__process_status_adjustment_http_errors(
    reward_status_adjustment_retry_task: RetryTask, adjustment_expected_payload: dict, adjustment_url: str
) -> None:

    for status, body in [
        (401, "Unauthorized"),
        (500, "Internal Server Error"),
    ]:
        httpretty.register_uri("PATCH", adjustment_url, body=body, status=status)

        with pytest.raises(requests.RequestException) as excinfo:
            _process_status_adjustment(reward_status_adjustment_retry_task.get_params())

        assert isinstance(excinfo.value, requests.RequestException)
        assert excinfo.value.response.status_code == status

        last_request = httpretty.last_request()
        assert last_request.method == "PATCH"
        assert json.loads(last_request.body) == adjustment_expected_payload


@mock.patch("app.tasks.status_adjustment.send_request_with_metrics")
def test__process_status_adjustment_connection_error(
    mock_send_request_with_metrics: mock.MagicMock, reward_status_adjustment_retry_task: RetryTask
) -> None:

    mock_send_request_with_metrics.side_effect = requests.Timeout("Request timed out")

    with pytest.raises(requests.RequestException) as excinfo:
        _process_status_adjustment(reward_status_adjustment_retry_task.get_params())

    assert isinstance(excinfo.value, requests.Timeout)
    assert excinfo.value.response is None


@httpretty.activate
def test_status_adjustment(
    db_session: "Session", reward_status_adjustment_retry_task: RetryTask, adjustment_url: str
) -> None:

    httpretty.register_uri("PATCH", adjustment_url, body="OK", status=200)

    status_adjustment(reward_status_adjustment_retry_task.retry_task_id)

    db_session.refresh(reward_status_adjustment_retry_task)

    assert reward_status_adjustment_retry_task.attempts == 1
    assert reward_status_adjustment_retry_task.next_attempt_time is None
    assert reward_status_adjustment_retry_task.status == RetryTaskStatuses.SUCCESS


def test_status_adjustment_wrong_status(db_session: "Session", reward_status_adjustment_retry_task: RetryTask) -> None:
    reward_status_adjustment_retry_task.status = RetryTaskStatuses.FAILED
    db_session.commit()

    with pytest.raises(IncorrectRetryTaskStatusError):
        status_adjustment(reward_status_adjustment_retry_task.retry_task_id)

    db_session.refresh(reward_status_adjustment_retry_task)

    assert reward_status_adjustment_retry_task.attempts == 0
    assert reward_status_adjustment_retry_task.next_attempt_time is None
    assert reward_status_adjustment_retry_task.status == RetryTaskStatuses.FAILED


def test_delete_unallocated_rewards(
    delete_rewards_retry_task: RetryTask, db_session: Session, reward: Reward
) -> None:
    task_params = delete_rewards_retry_task.get_params()

    other_config = RewardConfig(
        reward_slug="other-config",
        validity_days=15,
        retailer_slug=task_params["retailer_slug"],
    )
    db_session.add(other_config)
    db_session.flush()

    other_reward = Reward(
        code="sample-other-code",
        reward_config_id=other_config.id,
        retailer_slug=other_config.retailer_slug,
    )
    db_session.add(other_reward)
    db_session.commit()

    assert reward.deleted is False
    delete_unallocated_rewards(delete_rewards_retry_task.retry_task_id)

    db_session.refresh(delete_rewards_retry_task)
    db_session.refresh(reward)
    db_session.refresh(other_reward)

    assert reward.deleted is True
    assert other_reward.deleted is False
    assert delete_rewards_retry_task.next_attempt_time is None
    assert delete_rewards_retry_task.attempts == 1
    assert delete_rewards_retry_task.audit_data == []


@httpretty.activate
@mock.patch("app.tasks.reward_cancellation.datetime")
def test_cancel_reward(mock_datetime: mock.Mock, db_session: Session, cancel_rewards_retry_task: RetryTask) -> None:
    mock_datetime.utcnow.return_value = fake_now
    task_params = cancel_rewards_retry_task.get_params()
    url = "{base_url}/bpl/loyalty/{retailer_slug}/rewards/{reward_slug}/cancel".format(
        base_url=settings.POLARIS_URL,
        retailer_slug=task_params["retailer_slug"],
        reward_slug=task_params["reward_slug"],
    )

    httpretty.register_uri("POST", url, body="OK", status=202)
    cancel_rewards(cancel_rewards_retry_task.retry_task_id)
    db_session.refresh(cancel_rewards_retry_task)

    last_request = httpretty.last_request()
    assert last_request.method == "POST"
    assert last_request.url == url
    assert last_request.body == b""
    assert cancel_rewards_retry_task.next_attempt_time is None
    assert cancel_rewards_retry_task.attempts == 1
    assert cancel_rewards_retry_task.audit_data[0] == {
        "timestamp": fake_now.isoformat(),
        "response": {
            "status": 202,
            "body": "OK",
        },
    }


@httpretty.activate
def test_reward_issuance_409_from_polaris(
    db_session: "Session", issuance_retry_task: RetryTask, create_reward: Callable
) -> None:
    """Test reward is deleted for the task (from the DB) and task retried on a 409 from Polaris"""
    # Get the associated reward
    reward = db_session.execute(
        select(Reward).where(Reward.id == issuance_retry_task.get_params()["reward_uuid"])
    ).scalar_one()

    httpretty.register_uri("POST", issuance_retry_task.get_params()["account_url"], body="Conflict", status=409)

    with pytest.raises(requests.RequestException) as excinfo:
        issue_reward(issuance_retry_task.retry_task_id)

    db_session.refresh(issuance_retry_task)

    assert isinstance(excinfo.value, requests.RequestException)
    assert excinfo.value.response.status_code == 409
    assert (
        # The error handler will set the status to RETRYING
        issuance_retry_task.status
        == RetryTaskStatuses.IN_PROGRESS
    )
    issuance_retry_task.status = RetryTaskStatuses.RETRYING
    db_session.commit()
    db_session.refresh(reward)

    assert "reward_uuid" not in issuance_retry_task.get_params()
    assert issuance_retry_task.attempts == 1
    assert issuance_retry_task.next_attempt_time is None  # Will be set by error handler
    # The reward should also have been set to allocated: True
    assert reward.allocated

    # Now simulate the job being run again, which should pick up and process a new reward that will not cause a 409
    httpretty.register_uri("POST", issuance_retry_task.get_params()["account_url"], body="OK", status=200)
    # Add new reward and check that it's allocated and marked as allocated
    create_reward(**{"code": "TSTCD5678"})
    issue_reward(issuance_retry_task.retry_task_id)
    db_session.refresh(issuance_retry_task)
    db_session.refresh(reward)
    assert issuance_retry_task.attempts == 2
    assert issuance_retry_task.next_attempt_time is None
    assert issuance_retry_task.status == RetryTaskStatuses.SUCCESS
    assert reward.allocated


@httpretty.activate
def test_reward_issuance_no_reward_but_one_available_and_409(
    db_session: "Session", issuance_retry_task_no_reward: RetryTask, mocker: MockerFixture, reward: Reward
) -> None:
    """Test reward id is deleted for task (from the DB) and task is retried on a 409 from Polaris,
    if we don't initially have a reward"""
    mock_queue = mocker.patch("app.tasks.issuance.enqueue_retry_task_delay")
    assert "reward_uuid" not in issuance_retry_task_no_reward.get_params()

    httpretty.register_uri(
        "POST", issuance_retry_task_no_reward.get_params()["account_url"], body="Conflict", status=409
    )

    with pytest.raises(requests.RequestException) as excinfo:
        issue_reward(issuance_retry_task_no_reward.retry_task_id)

    assert isinstance(excinfo.value, requests.RequestException)
    assert excinfo.value.response.status_code == 409

    db_session.refresh(issuance_retry_task_no_reward)
    db_session.refresh(reward)

    assert not mock_queue.return_value.enqueue_at.called
    assert "reward_uuid" not in issuance_retry_task_no_reward.get_params()
    assert issuance_retry_task_no_reward.attempts == 1
    assert issuance_retry_task_no_reward.next_attempt_time is None
    assert issuance_retry_task_no_reward.status == RetryTaskStatuses.IN_PROGRESS
    # The reward should also have been set to allocated: True
    assert reward.allocated
