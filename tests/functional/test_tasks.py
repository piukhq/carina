import json

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from unittest import mock

import httpretty
import pytest
import requests

from pytest_mock import MockerFixture
from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import IncorrectRetryTaskStatusError, sync_create_task
from sqlalchemy.future import select
from sqlalchemy.orm import Session
from testfixtures import LogCapture

from carina.enums import RewardCampaignStatuses
from carina.models import Reward, RewardCampaign, RewardConfig
from carina.tasks.issuance import _process_issuance, issue_reward
from carina.tasks.status_adjustment import _process_status_adjustment, status_adjustment

fake_now = datetime.now(tz=timezone.utc)


@httpretty.activate
def test__process_issuance_ok(
    mocker: MockerFixture,
    reward_config: RewardConfig,
    reward_issuance_task_params: dict,
    issuance_expected_payload: dict,
) -> None:
    mock_send_activity = mocker.patch("carina.tasks.issuance.sync_send_activity")
    mock_datetime = mocker.patch("carina.tasks.issuance.datetime")

    sample_url = "http://sample.url"
    issuance_expected_payload["associated_url"] = sample_url
    validity_days = reward_config.load_required_fields_values()["validity_days"]
    reward_issuance_task_params["agent_state_params_raw"] = json.dumps({"associated_url": sample_url})

    mock_datetime.now.return_value = fake_now
    mock_issued_date = fake_now.timestamp()
    mock_expiry_date = (fake_now + timedelta(days=validity_days)).timestamp()
    issuance_expected_payload["issued_date"] = mock_issued_date
    issuance_expected_payload["expiry_date"] = mock_expiry_date

    httpretty.register_uri("POST", reward_issuance_task_params["account_url"], body="OK", status=200)

    response_audit = _process_issuance(reward_issuance_task_params, validity_days)

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
    mock_send_activity.assert_called_once()


@httpretty.activate
def test__process_issuance_fixed_expiry_date_ok(
    mocker: MockerFixture,
    reward_config: RewardConfig,
    reward_issuance_task_params: dict,
    issuance_expected_payload: dict,
) -> None:
    mock_send_activity = mocker.patch("carina.tasks.issuance.sync_send_activity")
    mock_datetime = mocker.patch("carina.tasks.issuance.datetime")

    sample_url = "http://sample.url"
    issuance_expected_payload["associated_url"] = sample_url
    validity_days = reward_config.load_required_fields_values()["validity_days"]
    reward_issuance_task_params["agent_state_params_raw"] = json.dumps({"associated_url": sample_url})

    mock_datetime.now.return_value = fake_now
    mock_issued_date = fake_now.timestamp()
    mock_expiry_date = (fake_now + timedelta(days=validity_days + 5)).timestamp()
    issuance_expected_payload["issued_date"] = mock_issued_date
    issuance_expected_payload["expiry_date"] = mock_expiry_date
    reward_issuance_task_params["expiry_date"] = mock_expiry_date

    httpretty.register_uri("POST", reward_issuance_task_params["account_url"], body="OK", status=200)

    response_audit = _process_issuance(reward_issuance_task_params, validity_days)

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
    mock_send_activity.assert_called_once()


@httpretty.activate
def test__process_issuance_http_errors(
    mocker: MockerFixture,
    reward_config: RewardConfig,
    reward_issuance_task_params: dict,
    issuance_expected_payload: dict,
) -> None:
    mock_send_activity = mocker.patch("carina.tasks.issuance.sync_send_activity")
    mock_datetime = mocker.patch("carina.tasks.issuance.datetime")
    mock_datetime.now.return_value = fake_now

    validity_days = reward_config.load_required_fields_values()["validity_days"]
    mock_issued_date = fake_now.timestamp()
    mock_expiry_date = (fake_now + timedelta(days=validity_days)).timestamp()
    issuance_expected_payload["issued_date"] = mock_issued_date
    issuance_expected_payload["expiry_date"] = mock_expiry_date

    for status, body in (
        (401, "Unauthorized"),
        (500, "Internal Server Error"),
    ):
        httpretty.register_uri("POST", reward_issuance_task_params["account_url"], body=body, status=status)

        with pytest.raises(requests.RequestException) as excinfo:
            _process_issuance(reward_issuance_task_params, validity_days)

        assert isinstance(excinfo.value, requests.RequestException)
        assert excinfo.value.response.status_code == status  # type: ignore [union-attr]

        last_request = httpretty.last_request()
        assert last_request.method == "POST"
        assert json.loads(last_request.body) == issuance_expected_payload

    mock_send_activity.assert_not_called()


def test__process_issuance_connection_error(mocker: MockerFixture, reward_issuance_task_params: dict) -> None:
    mock_send_activity = mocker.patch("carina.tasks.issuance.sync_send_activity")
    mocker.patch("carina.tasks.issuance.send_request_with_metrics", side_effect=requests.Timeout("Request timed out"))

    with pytest.raises(requests.RequestException) as excinfo:
        _process_issuance(reward_issuance_task_params, 1)

    assert isinstance(excinfo.value, requests.Timeout)
    assert excinfo.value.response is None
    mock_send_activity.assert_not_called()


@httpretty.activate
def test_reward_issuance(
    mocker: MockerFixture,
    db_session: "Session",
    reward_config: RewardConfig,
    issuance_retry_task: RetryTask,
    reward_campaign: RewardCampaign,
) -> None:
    mock_send_activity = mocker.patch("carina.tasks.issuance.sync_send_activity")
    httpretty.register_uri("POST", issuance_retry_task.get_params()["account_url"], body="OK", status=200)

    issue_reward(issuance_retry_task.retry_task_id)

    db_session.refresh(issuance_retry_task)

    assert issuance_retry_task.attempts == 1
    assert issuance_retry_task.next_attempt_time is None
    assert issuance_retry_task.status == RetryTaskStatuses.SUCCESS
    mock_send_activity.assert_called_once()


def test_reward_issuance_wrong_status(
    db_session: "Session",
    issuance_retry_task: RetryTask,
    reward_campaign: RewardCampaign,
) -> None:
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
    db_session: "Session",
    issuance_retry_task: RetryTask,
    mocker: MockerFixture,
    reward_campaign: RewardCampaign,
) -> None:
    """
    Test that, if the campaign has been cancelled by the time we get to issue a reward, the issuance is also cancelled
    """
    reward_campaign.campaign_status = RewardCampaignStatuses.CANCELLED
    db_session.commit()
    import carina.tasks.issuance as tasks_allocation

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
    mocker: MockerFixture,
    reward_campaign: RewardCampaign,
) -> None:
    """
    Test that, if the campaign has been cancelled by the time we get to issue a reward, the issuance is also cancelled.
    This is the test for when the task exists but a reward has not yet become available
    """
    reward_campaign.campaign_status = RewardCampaignStatuses.CANCELLED
    db_session.commit()
    import carina.tasks.issuance as tasks_allocation

    spy = mocker.spy(tasks_allocation, "_process_issuance")

    issue_reward(issuance_retry_task_no_reward.retry_task_id)

    db_session.refresh(issuance_retry_task_no_reward)

    assert issuance_retry_task_no_reward.attempts == 1
    assert issuance_retry_task_no_reward.next_attempt_time is None
    assert issuance_retry_task_no_reward.status == RetryTaskStatuses.CANCELLED
    assert spy.call_count == 0


@httpretty.activate
def test_reward_issuance_no_reward_but_one_available(
    db_session: "Session",
    issuance_retry_task_no_reward: RetryTask,
    mocker: MockerFixture,
    reward: Reward,
    reward_campaign: RewardCampaign,
) -> None:
    """test that an allocable reward (the pytest 'reward' fixture) is allocated, resulting in success"""
    mock_send_activity = mocker.patch("carina.tasks.issuance.sync_send_activity")
    mock_queue = mocker.patch("carina.tasks.issuance.enqueue_retry_task_delay")

    httpretty.register_uri("POST", issuance_retry_task_no_reward.get_params()["account_url"], body="OK", status=200)

    issue_reward(issuance_retry_task_no_reward.retry_task_id)

    db_session.refresh(issuance_retry_task_no_reward)

    assert not mock_queue.return_value.enqueue_at.called
    assert issuance_retry_task_no_reward.attempts == 1
    assert issuance_retry_task_no_reward.next_attempt_time is None
    assert issuance_retry_task_no_reward.status == RetryTaskStatuses.SUCCESS
    mock_send_activity.assert_called_once()


@httpretty.activate
def test_reward_issuance_no_reward_and_allocation_is_requeued(
    db_session: "Session",
    issuance_retry_task_no_reward: RetryTask,
    capture: LogCapture,
    mocker: MockerFixture,
    create_reward: Callable,
    reward_campaign: RewardCampaign,
) -> None:
    """test that no allocable reward results in the allocation being requeued"""
    mock_send_activity = mocker.patch("carina.tasks.issuance.sync_send_activity")
    mock_queue = mocker.patch("carina.tasks.issuance.enqueue_retry_task_delay")
    mock_queue.return_value = fake_now
    from carina.tasks.issuance import sentry_sdk as mock_sentry_sdk

    mock_settings = mocker.patch("carina.tasks.issuance.settings")
    mock_settings.MESSAGE_IF_NO_PRE_LOADED_REWARDS = True
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
    assert issuance_retry_task_no_reward.attempts == 1
    assert issuance_retry_task_no_reward.next_attempt_time is None
    assert issuance_retry_task_no_reward.status == RetryTaskStatuses.SUCCESS
    assert issuance_retry_task_no_reward.get_params()["reward_uuid"] == str(reward.id)
    assert reward.allocated
    mock_send_activity.assert_called_once()


@httpretty.activate
@mock.patch("carina.tasks.status_adjustment.datetime")
def test__process_status_adjustment_ok(
    mock_datetime: mock.Mock,
    db_session: "Session",
    reward_status_adjustment_retry_task: RetryTask,
    adjustment_expected_payload: dict,
    adjustment_url: str,
) -> None:
    mock_datetime.now.return_value = fake_now
    mock_datetime.fromisoformat = datetime.fromisoformat

    httpretty.register_uri("PATCH", adjustment_url, body="OK", status=200)

    response_audit = _process_status_adjustment(db_session, reward_status_adjustment_retry_task.get_params())

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
    db_session: "Session",
    reward_status_adjustment_retry_task: RetryTask,
    adjustment_expected_payload: dict,
    adjustment_url: str,
) -> None:
    for status, body in (
        (401, "Unauthorized"),
        (500, "Internal Server Error"),
    ):
        httpretty.register_uri("PATCH", adjustment_url, body=body, status=status)

        with pytest.raises(requests.RequestException) as excinfo:
            _process_status_adjustment(db_session, reward_status_adjustment_retry_task.get_params())

        assert isinstance(excinfo.value, requests.RequestException)
        assert excinfo.value.response.status_code == status  # type: ignore [union-attr]

        last_request = httpretty.last_request()
        assert last_request.method == "PATCH"
        assert json.loads(last_request.body) == adjustment_expected_payload


@httpretty.activate
def test__process_status_adjustment_404_not_found_soft_delete(
    db_session: "Session",
    reward_status_adjustment_retry_task: RetryTask,
    adjustment_expected_payload: dict,
    adjustment_url: str,
    reward: Reward,
) -> None:
    reward.allocated = True

    for status, body in ((404, "Not Found for Url"),):
        httpretty.register_uri("PATCH", adjustment_url, body=body, status=status)

        with pytest.raises(requests.RequestException) as excinfo:
            _process_status_adjustment(db_session, reward_status_adjustment_retry_task.get_params())

        assert isinstance(excinfo.value, requests.RequestException)
        assert excinfo.value.response.status_code == status  # type: ignore [union-attr]

        last_request = httpretty.last_request()
        assert last_request.method == "PATCH"
        assert json.loads(last_request.body) == adjustment_expected_payload

    db_session.refresh(reward)
    assert reward.deleted is True


@mock.patch("carina.tasks.status_adjustment.send_request_with_metrics")
def test__process_status_adjustment_connection_error(
    mock_send_request_with_metrics: mock.MagicMock,
    db_session: "Session",
    reward_status_adjustment_retry_task: RetryTask,
) -> None:
    mock_send_request_with_metrics.side_effect = requests.Timeout("Request timed out")

    with pytest.raises(requests.RequestException) as excinfo:
        _process_status_adjustment(db_session, reward_status_adjustment_retry_task.get_params())

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


@httpretty.activate
def test_reward_issuance_409_from_polaris(
    db_session: "Session",
    issuance_retry_task: RetryTask,
    create_reward: Callable,
    reward_issuance_task_params: dict,
    mocker: MockerFixture,
    reward_campaign: RewardCampaign,
) -> None:
    """Test reward is deleted for the task (from the DB) and task retried on a 409 from Polaris"""
    mock_send_activity = mocker.patch("carina.tasks.issuance.sync_send_activity")
    control_task = sync_create_task(
        db_session, task_type_name=issuance_retry_task.task_type.name, params=reward_issuance_task_params
    )
    db_session.commit()

    # Get the associated reward
    reward = db_session.execute(
        select(Reward).where(Reward.id == issuance_retry_task.get_params()["reward_uuid"])
    ).scalar_one()

    httpretty.register_uri("POST", issuance_retry_task.get_params()["account_url"], body="Conflict", status=409)

    with pytest.raises(requests.RequestException) as excinfo:
        issue_reward(issuance_retry_task.retry_task_id)

    db_session.refresh(issuance_retry_task)

    assert isinstance(excinfo.value, requests.RequestException)
    assert excinfo.value.response.status_code == 409  # type: ignore [union-attr]
    assert (
        # The error handler will set the status to RETRYING
        issuance_retry_task.status
        == RetryTaskStatuses.IN_PROGRESS
    )
    issuance_retry_task.status = RetryTaskStatuses.RETRYING
    db_session.commit()
    db_session.refresh(reward)

    assert all(item not in issuance_retry_task.get_params() for item in ("reward_uuid", "code"))
    assert all(item in control_task.get_params() for item in ("reward_uuid", "code"))

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
    mock_send_activity.assert_called_once()


@httpretty.activate
def test_reward_issuance_no_reward_but_one_available_and_409(
    db_session: "Session",
    issuance_retry_task_no_reward: RetryTask,
    mocker: MockerFixture,
    reward: Reward,
    reward_campaign: RewardCampaign,
) -> None:
    """Test reward id is deleted for task (from the DB) and task is retried on a 409 from Polaris,
    if we don't initially have a reward"""
    mock_send_activity = mocker.patch("carina.tasks.issuance.sync_send_activity")
    mock_queue = mocker.patch("carina.tasks.issuance.enqueue_retry_task_delay")
    assert "reward_uuid" not in issuance_retry_task_no_reward.get_params()

    httpretty.register_uri(
        "POST", issuance_retry_task_no_reward.get_params()["account_url"], body="Conflict", status=409
    )

    with pytest.raises(requests.RequestException) as excinfo:
        issue_reward(issuance_retry_task_no_reward.retry_task_id)

    assert isinstance(excinfo.value, requests.RequestException)
    assert excinfo.value.response.status_code == 409  # type: ignore [union-attr]

    db_session.refresh(issuance_retry_task_no_reward)
    db_session.refresh(reward)

    assert not mock_queue.return_value.enqueue_at.called
    assert "reward_uuid" not in issuance_retry_task_no_reward.get_params()
    assert issuance_retry_task_no_reward.attempts == 1
    assert issuance_retry_task_no_reward.next_attempt_time is None
    assert issuance_retry_task_no_reward.status == RetryTaskStatuses.IN_PROGRESS
    # The reward should also have been set to allocated: True
    assert reward.allocated
    mock_send_activity.assert_not_called()
