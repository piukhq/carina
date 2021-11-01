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
from sqlalchemy.future import select
from sqlalchemy.orm import Session
from testfixtures import LogCapture

from app.enums import VoucherTypeStatuses
from app.models import Voucher, VoucherConfig
from app.tasks.allocation import _process_issuance, issue_voucher
from app.tasks.status_adjustment import _process_status_adjustment, status_adjustment

fake_now = datetime.utcnow()


@httpretty.activate
@mock.patch("app.tasks.allocation.datetime")
def test__process_issuance_ok(
    mock_datetime: mock.Mock, voucher_issuance_task_params: dict, issuance_expected_payload: dict
) -> None:

    mock_datetime.utcnow.return_value = fake_now
    httpretty.register_uri("POST", voucher_issuance_task_params["account_url"], body="OK", status=200)

    response_audit = _process_issuance(voucher_issuance_task_params)

    last_request = httpretty.last_request()
    assert last_request.method == "POST"
    assert last_request.url == voucher_issuance_task_params["account_url"]
    assert json.loads(last_request.body) == issuance_expected_payload
    assert response_audit == {
        "timestamp": fake_now.isoformat(),
        "response": {
            "status": 200,
            "body": "OK",
        },
    }


@httpretty.activate
def test__process_issuance_http_errors(voucher_issuance_task_params: dict, issuance_expected_payload: dict) -> None:

    for status, body in [
        (401, "Unauthorized"),
        (500, "Internal Server Error"),
    ]:
        httpretty.register_uri("POST", voucher_issuance_task_params["account_url"], body=body, status=status)

        with pytest.raises(requests.RequestException) as excinfo:
            _process_issuance(voucher_issuance_task_params)

        assert isinstance(excinfo.value, requests.RequestException)
        assert excinfo.value.response.status_code == status

        last_request = httpretty.last_request()
        assert last_request.method == "POST"
        assert json.loads(last_request.body) == issuance_expected_payload


@mock.patch("app.tasks.allocation.send_request_with_metrics")
def test__process_issuance_connection_error(
    mock_send_request_with_metrics: mock.MagicMock, voucher_issuance_task_params: dict
) -> None:

    mock_send_request_with_metrics.side_effect = requests.Timeout("Request timed out")

    with pytest.raises(requests.RequestException) as excinfo:
        _process_issuance(voucher_issuance_task_params)

    assert isinstance(excinfo.value, requests.Timeout)
    assert excinfo.value.response is None


@httpretty.activate
def test_voucher_issuance(db_session: "Session", issuance_retry_task: RetryTask) -> None:
    issuance_retry_task.status = RetryTaskStatuses.IN_PROGRESS
    db_session.commit()

    httpretty.register_uri("POST", issuance_retry_task.get_params()["account_url"], body="OK", status=200)

    issue_voucher(issuance_retry_task.retry_task_id)

    db_session.refresh(issuance_retry_task)

    assert issuance_retry_task.attempts == 1
    assert issuance_retry_task.next_attempt_time is None
    assert issuance_retry_task.status == RetryTaskStatuses.SUCCESS


def test_voucher_issuance_wrong_status(db_session: "Session", issuance_retry_task: RetryTask) -> None:
    issuance_retry_task.status = RetryTaskStatuses.FAILED
    db_session.commit()

    with pytest.raises(ValueError):
        issue_voucher(issuance_retry_task.retry_task_id)

    db_session.refresh(issuance_retry_task)

    assert issuance_retry_task.attempts == 0
    assert issuance_retry_task.next_attempt_time is None
    assert issuance_retry_task.status == RetryTaskStatuses.FAILED


@httpretty.activate
def test_voucher_issuance_campaign_is_cancelled(
    db_session: "Session", issuance_retry_task: RetryTask, voucher_config: VoucherConfig, mocker: MockerFixture
) -> None:
    """
    Test that, if the campaign has been cancelled by the time we get to issue a voucher, the issuance is also cancelled
    """
    issuance_retry_task.status = RetryTaskStatuses.IN_PROGRESS
    db_session.commit()
    voucher_config.status = VoucherTypeStatuses.CANCELLED
    db_session.commit()
    import app.tasks.allocation as tasks_allocation

    spy = mocker.spy(tasks_allocation, "_process_issuance")

    issue_voucher(issuance_retry_task.retry_task_id)

    db_session.refresh(issuance_retry_task)

    assert issuance_retry_task.attempts == 0
    assert issuance_retry_task.next_attempt_time is None
    assert issuance_retry_task.status == RetryTaskStatuses.CANCELLED
    assert spy.call_count == 0
    voucher = db_session.execute(
        select(Voucher).where(Voucher.id == issuance_retry_task.get_params()["voucher_id"])
    ).scalar_one()
    db_session.refresh(voucher)
    assert not voucher.allocated
    assert voucher.deleted


@httpretty.activate
def test_voucher_issuance_no_voucher_campaign_is_cancelled(
    db_session: "Session",
    issuance_retry_task_no_voucher: RetryTask,
    voucher_config: VoucherConfig,
    mocker: MockerFixture,
) -> None:
    """
    Test that, if the campaign has been cancelled by the time we get to issue a voucher, the issuance is also cancelled.
    This is the test for when the task exists but a voucher has not yet become available
    """
    issuance_retry_task_no_voucher.status = RetryTaskStatuses.IN_PROGRESS
    db_session.commit()
    voucher_config.status = VoucherTypeStatuses.CANCELLED
    db_session.commit()
    import app.tasks.allocation as tasks_allocation

    spy = mocker.spy(tasks_allocation, "_process_issuance")

    issue_voucher(issuance_retry_task_no_voucher.retry_task_id)

    db_session.refresh(issuance_retry_task_no_voucher)

    assert issuance_retry_task_no_voucher.attempts == 0
    assert issuance_retry_task_no_voucher.next_attempt_time is None
    assert issuance_retry_task_no_voucher.status == RetryTaskStatuses.CANCELLED
    assert spy.call_count == 0


@httpretty.activate
def test_voucher_issuance_no_voucher_but_one_available(
    db_session: "Session", issuance_retry_task_no_voucher: RetryTask, mocker: MockerFixture, voucher: Voucher
) -> None:
    """test that an allocable voucher (the pytest 'voucher' fixture) is allocated, resulting in success"""
    mock_queue = mocker.patch("app.tasks.allocation.enqueue_retry_task_delay")
    issuance_retry_task_no_voucher.status = RetryTaskStatuses.IN_PROGRESS
    db_session.commit()

    httpretty.register_uri("POST", issuance_retry_task_no_voucher.get_params()["account_url"], body="OK", status=200)

    issue_voucher(issuance_retry_task_no_voucher.retry_task_id)

    db_session.refresh(issuance_retry_task_no_voucher)

    assert not mock_queue.return_value.enqueue_at.called
    assert issuance_retry_task_no_voucher.attempts == 1
    assert issuance_retry_task_no_voucher.next_attempt_time is None
    assert issuance_retry_task_no_voucher.status == RetryTaskStatuses.SUCCESS


@httpretty.activate
def test_voucher_issuance_no_voucher_and_allocation_is_requeued(
    db_session: "Session",
    issuance_retry_task_no_voucher: RetryTask,
    capture: LogCapture,
    mocker: MockerFixture,
    create_voucher: Callable,
) -> None:
    """test that no allocable voucher results in the allocation being requeued"""
    mock_queue = mocker.patch("app.tasks.allocation.enqueue_retry_task_delay")
    mock_queue.return_value = fake_now
    from app.tasks.allocation import sentry_sdk as mock_sentry_sdk

    sentry_spy = mocker.spy(mock_sentry_sdk, "capture_message")
    issuance_retry_task_no_voucher.status = RetryTaskStatuses.IN_PROGRESS
    db_session.commit()

    httpretty.register_uri("POST", issuance_retry_task_no_voucher.get_params()["account_url"], body="OK", status=200)

    issue_voucher(issuance_retry_task_no_voucher.retry_task_id)

    db_session.refresh(issuance_retry_task_no_voucher)
    mock_queue.assert_called_once()
    sentry_spy.assert_called_once()
    assert issuance_retry_task_no_voucher.attempts == 1
    assert issuance_retry_task_no_voucher.next_attempt_time is not None
    assert issuance_retry_task_no_voucher.status == RetryTaskStatuses.WAITING
    assert any("Next attempt time at" in record.msg for record in capture.records)

    # Add new voucher and check that it's allocated and marked as allocated
    voucher = create_voucher()  # The defaults will be correct for this test

    # call allocate_voucher again
    issue_voucher(issuance_retry_task_no_voucher.retry_task_id)

    db_session.refresh(issuance_retry_task_no_voucher)
    db_session.refresh(voucher)
    mock_queue.assert_called_once()  # should not have been called again
    assert issuance_retry_task_no_voucher.attempts == 2
    assert issuance_retry_task_no_voucher.next_attempt_time is None
    assert issuance_retry_task_no_voucher.status == RetryTaskStatuses.SUCCESS
    assert issuance_retry_task_no_voucher.get_params()["voucher_id"] == str(voucher.id)
    assert voucher.allocated


@httpretty.activate
@mock.patch("app.tasks.status_adjustment.datetime")
def test__process_status_adjustment_ok(
    mock_datetime: mock.Mock,
    voucher_status_adjustment_retry_task: RetryTask,
    adjustment_expected_payload: dict,
    adjustment_url: str,
) -> None:

    mock_datetime.utcnow.return_value = fake_now
    mock_datetime.fromisoformat = datetime.fromisoformat

    httpretty.register_uri("PATCH", adjustment_url, body="OK", status=200)

    response_audit = _process_status_adjustment(voucher_status_adjustment_retry_task.get_params())

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
    voucher_status_adjustment_retry_task: RetryTask, adjustment_expected_payload: dict, adjustment_url: str
) -> None:

    for status, body in [
        (401, "Unauthorized"),
        (500, "Internal Server Error"),
    ]:
        httpretty.register_uri("PATCH", adjustment_url, body=body, status=status)

        with pytest.raises(requests.RequestException) as excinfo:
            _process_status_adjustment(voucher_status_adjustment_retry_task.get_params())

        assert isinstance(excinfo.value, requests.RequestException)
        assert excinfo.value.response.status_code == status

        last_request = httpretty.last_request()
        assert last_request.method == "PATCH"
        assert json.loads(last_request.body) == adjustment_expected_payload


@mock.patch("app.tasks.status_adjustment.send_request_with_metrics")
def test__process_status_adjustment_connection_error(
    mock_send_request_with_metrics: mock.MagicMock, voucher_status_adjustment_retry_task: RetryTask
) -> None:

    mock_send_request_with_metrics.side_effect = requests.Timeout("Request timed out")

    with pytest.raises(requests.RequestException) as excinfo:
        _process_status_adjustment(voucher_status_adjustment_retry_task.get_params())

    assert isinstance(excinfo.value, requests.Timeout)
    assert excinfo.value.response is None


@httpretty.activate
def test_status_adjustment(
    db_session: "Session", voucher_status_adjustment_retry_task: RetryTask, adjustment_url: str
) -> None:
    voucher_status_adjustment_retry_task.status = RetryTaskStatuses.IN_PROGRESS
    db_session.commit()

    httpretty.register_uri("PATCH", adjustment_url, body="OK", status=200)

    status_adjustment(voucher_status_adjustment_retry_task.retry_task_id)

    db_session.refresh(voucher_status_adjustment_retry_task)

    assert voucher_status_adjustment_retry_task.attempts == 1
    assert voucher_status_adjustment_retry_task.next_attempt_time is None
    assert voucher_status_adjustment_retry_task.status == RetryTaskStatuses.SUCCESS


def test_status_adjustment_wrong_status(db_session: "Session", voucher_status_adjustment_retry_task: RetryTask) -> None:
    voucher_status_adjustment_retry_task.status = RetryTaskStatuses.FAILED
    db_session.commit()

    with pytest.raises(ValueError):
        status_adjustment(voucher_status_adjustment_retry_task.retry_task_id)

    db_session.refresh(voucher_status_adjustment_retry_task)

    assert voucher_status_adjustment_retry_task.attempts == 0
    assert voucher_status_adjustment_retry_task.next_attempt_time is None
    assert voucher_status_adjustment_retry_task.status == RetryTaskStatuses.FAILED
