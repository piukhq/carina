import json

from datetime import datetime
from unittest import mock

import httpretty
import pytest
import requests

from sqlalchemy.orm import Session

from app.enums import QueuedRetryStatuses
from app.models import VoucherAllocation
from app.models.voucher import VoucherUpdate
from app.tasks.allocation import _process_allocation, allocate_voucher
from app.tasks.status_adjustment import _process_status_adjustment, status_adjustment
from app.tasks.voucher import enqueue_voucher_allocation

fake_now = datetime.utcnow()


@pytest.mark.asyncio
@mock.patch("rq.Queue")
async def test_enqueue_reward_adjustment_task(
    MockQueue: mock.MagicMock, db_session: "Session", voucher_allocation: VoucherAllocation
) -> None:

    mock_queue = MockQueue.return_value

    await enqueue_voucher_allocation(voucher_allocation.id)

    MockQueue.call_args[0] == "bpl_voucher_adjustments"
    mock_queue.enqueue.assert_called_once()
    db_session.refresh(voucher_allocation)
    assert voucher_allocation.status == QueuedRetryStatuses.IN_PROGRESS


@pytest.mark.asyncio
@mock.patch("rq.Queue")
async def test_enqueue_reward_adjustment_task_no_voucher(
    MockQueue: mock.MagicMock, db_session: "Session", voucher_allocation: VoucherAllocation
) -> None:

    voucher_allocation.voucher_id = None
    db_session.commit()

    mock_queue = MockQueue.return_value

    await enqueue_voucher_allocation(voucher_allocation.id)

    MockQueue.call_args[0] == "bpl_voucher_adjustments"
    mock_queue.enqueue.assert_not_called()
    db_session.refresh(voucher_allocation)
    assert voucher_allocation.status == QueuedRetryStatuses.FAILED


@httpretty.activate
@mock.patch("app.tasks.allocation.datetime")
def test__process_allocation_ok(
    mock_datetime: mock.Mock,
    voucher_allocation: VoucherAllocation,
    allocation_expected_payload: dict,
) -> None:

    mock_datetime.utcnow.return_value = fake_now
    httpretty.register_uri("POST", voucher_allocation.account_url, body="OK", status=200)

    response_audit = _process_allocation(voucher_allocation)

    last_request = httpretty.last_request()
    assert last_request.method == "POST"
    assert last_request.url == voucher_allocation.account_url
    assert json.loads(last_request.body) == allocation_expected_payload
    assert response_audit == {
        "timestamp": fake_now.isoformat(),
        "response": {
            "status": 200,
            "body": "OK",
        },
    }


@httpretty.activate
def test__process_allocation_http_errors(
    voucher_allocation: VoucherAllocation, allocation_expected_payload: dict
) -> None:

    for status, body in [
        (401, "Unauthorized"),
        (500, "Internal Server Error"),
    ]:
        httpretty.register_uri("POST", voucher_allocation.account_url, body=body, status=status)

        with pytest.raises(requests.RequestException) as excinfo:
            _process_allocation(voucher_allocation)

        assert isinstance(excinfo.value, requests.RequestException)
        assert excinfo.value.response.status_code == status

        last_request = httpretty.last_request()
        assert last_request.method == "POST"
        assert json.loads(last_request.body) == allocation_expected_payload


@mock.patch("app.tasks.allocation.send_request_with_metrics")
def test__process_allocation_connection_error(
    mock_send_request_with_metrics: mock.MagicMock,
    voucher_allocation: VoucherAllocation,
) -> None:

    mock_send_request_with_metrics.side_effect = requests.Timeout("Request timed out")

    with pytest.raises(requests.RequestException) as excinfo:
        _process_allocation(voucher_allocation)

    assert isinstance(excinfo.value, requests.Timeout)
    assert excinfo.value.response is None


@httpretty.activate
def test_allocate_voucher(db_session: "Session", voucher_allocation: VoucherAllocation) -> None:
    voucher_allocation.status = QueuedRetryStatuses.IN_PROGRESS  # type: ignore
    db_session.commit()

    httpretty.register_uri("POST", voucher_allocation.account_url, body="OK", status=200)

    allocate_voucher(voucher_allocation.id)

    db_session.refresh(voucher_allocation)

    assert voucher_allocation.attempts == 1
    assert voucher_allocation.next_attempt_time is None
    assert voucher_allocation.status == QueuedRetryStatuses.SUCCESS


def test_allocate_voucher_wrong_status(db_session: "Session", voucher_allocation: VoucherAllocation) -> None:
    voucher_allocation.status = QueuedRetryStatuses.FAILED  # type: ignore
    db_session.commit()

    with pytest.raises(ValueError):
        allocate_voucher(voucher_allocation.id)

    db_session.refresh(voucher_allocation)

    assert voucher_allocation.attempts == 0
    assert voucher_allocation.next_attempt_time is None
    assert voucher_allocation.status == QueuedRetryStatuses.FAILED


@httpretty.activate
@mock.patch("app.tasks.status_adjustment.datetime")
def test__process_status_adjustment_ok(
    mock_datetime: mock.Mock, voucher_update: VoucherUpdate, adjustment_expected_payload: dict, adjustment_url: str
) -> None:

    mock_datetime.utcnow.return_value = fake_now
    mock_datetime.fromisoformat = datetime.fromisoformat

    httpretty.register_uri("PATCH", adjustment_url, body="OK", status=200)

    response_audit = _process_status_adjustment(voucher_update)

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
    voucher_update: VoucherUpdate, adjustment_expected_payload: dict, adjustment_url: str
) -> None:

    for status, body in [
        (401, "Unauthorized"),
        (500, "Internal Server Error"),
    ]:
        httpretty.register_uri("PATCH", adjustment_url, body=body, status=status)

        with pytest.raises(requests.RequestException) as excinfo:
            _process_status_adjustment(voucher_update)

        assert isinstance(excinfo.value, requests.RequestException)
        assert excinfo.value.response.status_code == status

        last_request = httpretty.last_request()
        assert last_request.method == "PATCH"
        assert json.loads(last_request.body) == adjustment_expected_payload


@mock.patch("app.tasks.status_adjustment.send_request_with_metrics")
def test__process_status_adjustment_connection_error(
    mock_send_request_with_metrics: mock.MagicMock, voucher_update: VoucherUpdate
) -> None:

    mock_send_request_with_metrics.side_effect = requests.Timeout("Request timed out")

    with pytest.raises(requests.RequestException) as excinfo:
        _process_status_adjustment(voucher_update)

    assert isinstance(excinfo.value, requests.Timeout)
    assert excinfo.value.response is None


@httpretty.activate
def test_status_adjustment(db_session: "Session", voucher_update: VoucherUpdate, adjustment_url: str) -> None:
    voucher_update.retry_status = QueuedRetryStatuses.IN_PROGRESS  # type: ignore
    db_session.commit()

    httpretty.register_uri("PATCH", adjustment_url, body="OK", status=200)

    status_adjustment(voucher_update.id)

    db_session.refresh(voucher_update)

    assert voucher_update.attempts == 1
    assert voucher_update.next_attempt_time is None
    assert voucher_update.retry_status == QueuedRetryStatuses.SUCCESS


def test_status_adjustment_wrong_status(db_session: "Session", voucher_update: VoucherUpdate) -> None:
    voucher_update.retry_status = QueuedRetryStatuses.FAILED  # type: ignore
    db_session.commit()

    with pytest.raises(ValueError):
        status_adjustment(voucher_update.id)

    db_session.refresh(voucher_update)

    assert voucher_update.attempts == 0
    assert voucher_update.next_attempt_time is None
    assert voucher_update.retry_status == QueuedRetryStatuses.FAILED
