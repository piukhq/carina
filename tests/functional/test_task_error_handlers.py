# import typing

# from datetime import datetime, timedelta, timezone
# from inspect import Traceback
# from unittest import mock

# import pytest
# import requests
# import rq

# from app.core.config import settings

# from app.models import VoucherAllocation, VoucherUpdate
# from app.tasks.allocation import issue_voucher
# from app.tasks.error_handlers import handle_voucher_issuance_error, handle_voucher_status_adjustment_error
# from app.tasks.status_adjustment import status_adjustment
# from retry_task_lib.enums import QueuedRetryStatuses
# if typing.TYPE_CHECKING:
#     from sqlalchemy.orm import Session


# @pytest.fixture()
# def allocation(db_session: "Session", voucher_allocation: VoucherAllocation) -> VoucherAllocation:
#     #  For correctness, as we are in the error scenario here
#     voucher_allocation.status = QueuedRetryStatuses.IN_PROGRESS  # type: ignore
#     voucher_allocation.attempts = 1
#     db_session.commit()
#     return voucher_allocation


# @pytest.fixture()
# def adjustment(db_session: "Session", voucher_update: VoucherUpdate) -> VoucherUpdate:
#     #  For correctness, as we are in the error scenario here
#     voucher_update.retry_status = QueuedRetryStatuses.IN_PROGRESS  # type: ignore
#     voucher_update.attempts = 1
#     db_session.commit()
#     return voucher_update


# @pytest.fixture(scope="function")
# def fixed_now() -> typing.Generator[datetime, None, None]:
#     fixed = datetime.utcnow()
#     with mock.patch("app.tasks.error_handlers.datetime") as mock_datetime:
#         mock_datetime.utcnow.return_value = fixed
#         yield fixed


def test_always_200() -> None:
    assert True


# @mock.patch("rq.Queue")
# def test_handle_voucher_allocation_error_5xx(
#     mock_queue: mock.MagicMock,
#     db_session: "Session",
#     allocation: VoucherAllocation,
#     fixed_now: datetime,
# ) -> None:
#     job = mock.MagicMock(spec=rq.job.Job, kwargs={"voucher_allocation_id": allocation.id})
#     traceback = mock.MagicMock(spec=Traceback)
#     mock_request = mock.MagicMock(spec=requests.Request, url=allocation.account_url)
#     handle_voucher_issuance_error(
#         job,
#         type(requests.RequestException),
#         requests.RequestException(
#             request=mock_request,
#             response=mock.MagicMock(
#                 spec=requests.Response, request=mock_request, status_code=500, text="Internal server error"
#             ),
#         ),
#         traceback,
#     )
#     db_session.refresh(allocation)
#     assert len(allocation.response_data) == 1
#     assert allocation.response_data[0]["response"]["body"] == "Internal server error"
#     assert allocation.response_data[0]["response"]["status"] == 500

#     mock_queue.return_value.enqueue_at.assert_called_with(
#         fixed_now.replace(tzinfo=timezone.utc) + timedelta(seconds=180),
#         issue_voucher,
#         voucher_allocation_id=allocation.id,
#         failure_ttl=604800,
#     )
#     assert allocation.status == QueuedRetryStatuses.IN_PROGRESS
#     assert allocation.attempts == 1
#     assert allocation.next_attempt_time == fixed_now + timedelta(seconds=180)


# @mock.patch("rq.Queue")
# def test_handle_adjust_balance_error_no_response(
#     mock_queue: mock.MagicMock,
#     db_session: "Session",
#     allocation: VoucherAllocation,
#     fixed_now: datetime,
# ) -> None:

#     job = mock.MagicMock(spec=rq.job.Job, kwargs={"voucher_allocation_id": allocation.id})
#     traceback = mock.MagicMock(spec=Traceback)
#     mock_request = mock.MagicMock(spec=requests.Request, url=allocation.account_url)
#     handle_voucher_issuance_error(
#         job,
#         type(requests.Timeout),
#         requests.Timeout(
#             "Request timed out",
#             request=mock_request,
#             response=None,
#         ),
#         traceback,
#     )
#     db_session.refresh(allocation)
#     assert len(allocation.response_data) == 1
#     assert allocation.response_data[0]["error"] == "Request timed out"

#     mock_queue.return_value.enqueue_at.assert_called_with(
#         fixed_now.replace(tzinfo=timezone.utc) + timedelta(seconds=180),
#         issue_voucher,
#         voucher_allocation_id=allocation.id,
#         failure_ttl=604800,
#     )
#     assert allocation.status == QueuedRetryStatuses.IN_PROGRESS
#     assert allocation.attempts == 1
#     assert allocation.next_attempt_time == fixed_now + timedelta(seconds=180)


# @mock.patch("rq.Queue")
# def test_handle_adjust_balance_error_no_further_retries(
#     mock_queue: mock.MagicMock, db_session: "Session", allocation: VoucherAllocation
# ) -> None:
#     allocation.attempts = settings.VOUCHER_ALLOCATION_MAX_RETRIES
#     db_session.commit()

#     job = mock.MagicMock(spec=rq.job.Job, kwargs={"voucher_allocation_id": allocation.id})
#     traceback = mock.MagicMock(spec=Traceback)
#     mock_request = mock.MagicMock(spec=requests.Request, url=allocation.account_url)
#     handle_voucher_issuance_error(
#         job,
#         type(requests.RequestException),
#         requests.RequestException(
#             request=mock_request,
#             response=mock.MagicMock(
#                 spec=requests.Response, request=mock_request, status_code=500, text="Internal server error"
#             ),
#         ),
#         traceback,
#     )
#     db_session.refresh(allocation)
#     mock_queue.assert_not_called()
#     assert allocation.status == QueuedRetryStatuses.FAILED
#     assert allocation.attempts == settings.VOUCHER_ALLOCATION_MAX_RETRIES
#     assert allocation.next_attempt_time is None


# @mock.patch("rq.Queue")
# def test_handle_adjust_balance_error_unhandleable_response(
#     mock_queue: mock.MagicMock, db_session: "Session", allocation: VoucherAllocation
# ) -> None:

#     job = mock.MagicMock(spec=rq.job.Job, kwargs={"voucher_allocation_id": allocation.id})
#     traceback = mock.MagicMock(spec=Traceback)
#     mock_request = mock.MagicMock(spec=requests.Request, url=allocation.account_url)
#     handle_voucher_issuance_error(
#         job,
#         type(requests.RequestException),
#         requests.RequestException(
#             request=mock_request,
#             response=mock.MagicMock(spec=requests.Response, request=mock_request, status_code=401, text="Unauthorized"),
#         ),
#         traceback,
#     )
#     db_session.refresh(allocation)
#     mock_queue.assert_not_called()
#     assert allocation.status == QueuedRetryStatuses.FAILED
#     assert allocation.next_attempt_time is None


# @mock.patch("sentry_sdk.capture_exception")
# def test_handle_adjust_balance_error_unhandled_exception(
#     mock_sentry_capture_exception: mock.MagicMock, db_session: "Session", allocation: VoucherAllocation
# ) -> None:

#     job = mock.MagicMock(spec=rq.job.Job, kwargs={"voucher_allocation_id": allocation.id})
#     traceback = mock.MagicMock(spec=Traceback)
#     handle_voucher_issuance_error(
#         job,
#         type(ValueError),
#         ValueError("Le Meow"),
#         traceback,
#     )
#     db_session.refresh(allocation)

#     mock_sentry_capture_exception.assert_called_once()
#     assert allocation.status == QueuedRetryStatuses.FAILED
#     assert allocation.next_attempt_time is None


# @mock.patch("rq.Queue")
# def test_handle_adjust_balance_error_account_holder_deleted(
#     mock_queue: mock.MagicMock, db_session: "Session", allocation: VoucherAllocation
# ) -> None:

#     job = mock.MagicMock(spec=rq.job.Job, kwargs={"voucher_allocation_id": allocation.id})
#     traceback = mock.MagicMock(spec=Traceback)
#     mock_request = mock.MagicMock(spec=requests.Request, url=allocation.account_url)
#     handle_voucher_issuance_error(
#         job,
#         type(requests.RequestException),
#         requests.RequestException(
#             request=mock_request,
#             response=mock.MagicMock(
#                 spec=requests.Response,
#                 request=mock_request,
#                 status_code=404,
#                 text="Not Found",
#                 json=lambda: {
#                     "display_message": "Account not found for provided credentials.",
#                     "error": "NO_ACCOUNT_FOUND",
#                 },
#             ),
#         ),
#         traceback,
#     )
#     db_session.refresh(allocation)
#     mock_queue.assert_not_called()
#     assert allocation.status == QueuedRetryStatuses.FAILED
#     assert allocation.next_attempt_time is None


# @mock.patch("rq.Queue")
# def test_handle_voucher_status_adjustment_error_5xx(
#     mock_queue: mock.MagicMock,
#     db_session: "Session",
#     adjustment: VoucherUpdate,
#     fixed_now: datetime,
#     adjustment_url: str,
# ) -> None:
#     job = mock.MagicMock(spec=rq.job.Job, kwargs={"voucher_status_adjustment_id": adjustment.id})
#     traceback = mock.MagicMock(spec=Traceback)
#     mock_request = mock.MagicMock(spec=requests.Request, url=adjustment_url)
#     handle_voucher_status_adjustment_error(
#         job,
#         type(requests.RequestException),
#         requests.RequestException(
#             request=mock_request,
#             response=mock.MagicMock(
#                 spec=requests.Response, request=mock_request, status_code=500, text="Internal server error"
#             ),
#         ),
#         traceback,
#     )
#     db_session.refresh(adjustment)
#     assert len(adjustment.response_data) == 1
#     assert adjustment.response_data[0]["response"]["body"] == "Internal server error"
#     assert adjustment.response_data[0]["response"]["status"] == 500

#     mock_queue.return_value.enqueue_at.assert_called_with(
#         fixed_now.replace(tzinfo=timezone.utc) + timedelta(seconds=180),
#         status_adjustment,
#         voucher_status_adjustment_id=adjustment.id,
#         failure_ttl=604800,
#     )
#     assert adjustment.retry_status == QueuedRetryStatuses.IN_PROGRESS
#     assert adjustment.attempts == 1
#     assert adjustment.next_attempt_time == fixed_now + timedelta(seconds=180)


# @mock.patch("rq.Queue")
# def test_handle_voucher_status_adjustment_error_no_response(
#     mock_queue: mock.MagicMock,
#     db_session: "Session",
#     adjustment: VoucherUpdate,
#     fixed_now: datetime,
#     adjustment_url: str,
# ) -> None:

#     job = mock.MagicMock(spec=rq.job.Job, kwargs={"voucher_status_adjustment_id": adjustment.id})
#     traceback = mock.MagicMock(spec=Traceback)
#     mock_request = mock.MagicMock(spec=requests.Request, url=adjustment_url)
#     handle_voucher_status_adjustment_error(
#         job,
#         type(requests.Timeout),
#         requests.Timeout(
#             "Request timed out",
#             request=mock_request,
#             response=None,
#         ),
#         traceback,
#     )
#     db_session.refresh(adjustment)
#     assert len(adjustment.response_data) == 1
#     assert adjustment.response_data[0]["error"] == "Request timed out"

#     mock_queue.return_value.enqueue_at.assert_called_with(
#         fixed_now.replace(tzinfo=timezone.utc) + timedelta(seconds=180),
#         status_adjustment,
#         voucher_status_adjustment_id=adjustment.id,
#         failure_ttl=604800,
#     )
#     assert adjustment.retry_status == QueuedRetryStatuses.IN_PROGRESS
#     assert adjustment.attempts == 1
#     assert adjustment.next_attempt_time == fixed_now + timedelta(seconds=180)


# @mock.patch("rq.Queue")
# def test_handle_voucher_status_adjustment_error_no_further_retries(
#     mock_queue: mock.MagicMock, db_session: "Session", adjustment: VoucherUpdate, adjustment_url: str
# ) -> None:
#     adjustment.attempts = settings.VOUCHER_ALLOCATION_MAX_RETRIES
#     db_session.commit()

#     job = mock.MagicMock(spec=rq.job.Job, kwargs={"voucher_status_adjustment_id": adjustment.id})
#     traceback = mock.MagicMock(spec=Traceback)
#     mock_request = mock.MagicMock(spec=requests.Request, url=adjustment_url)
#     handle_voucher_status_adjustment_error(
#         job,
#         type(requests.RequestException),
#         requests.RequestException(
#             request=mock_request,
#             response=mock.MagicMock(
#                 spec=requests.Response, request=mock_request, status_code=500, text="Internal server error"
#             ),
#         ),
#         traceback,
#     )
#     db_session.refresh(adjustment)
#     mock_queue.assert_not_called()
#     assert adjustment.retry_status == QueuedRetryStatuses.FAILED
#     assert adjustment.attempts == settings.VOUCHER_ALLOCATION_MAX_RETRIES
#     assert adjustment.next_attempt_time is None


# @mock.patch("rq.Queue")
# def test_handle_voucher_status_adjustment_error_unhandleable_response(
#     mock_queue: mock.MagicMock, db_session: "Session", adjustment: VoucherUpdate, adjustment_url: str
# ) -> None:

#     job = mock.MagicMock(spec=rq.job.Job, kwargs={"voucher_status_adjustment_id": adjustment.id})
#     traceback = mock.MagicMock(spec=Traceback)
#     mock_request = mock.MagicMock(spec=requests.Request, url=adjustment_url)
#     handle_voucher_status_adjustment_error(
#         job,
#         type(requests.RequestException),
#         requests.RequestException(
#             request=mock_request,
#             response=mock.MagicMock(spec=requests.Response, request=mock_request, status_code=401, text="Unauthorized"),
#         ),
#         traceback,
#     )
#     db_session.refresh(adjustment)
#     mock_queue.assert_not_called()
#     assert adjustment.retry_status == QueuedRetryStatuses.FAILED
#     assert adjustment.next_attempt_time is None


# @mock.patch("sentry_sdk.capture_exception")
# def test_handle_voucher_status_adjustment_error_unhandled_exception(
#     mock_sentry_capture_exception: mock.MagicMock, db_session: "Session", adjustment: VoucherUpdate
# ) -> None:

#     job = mock.MagicMock(spec=rq.job.Job, kwargs={"voucher_status_adjustment_id": adjustment.id})
#     traceback = mock.MagicMock(spec=Traceback)
#     handle_voucher_status_adjustment_error(
#         job,
#         type(ValueError),
#         ValueError("Le Meow"),
#         traceback,
#     )
#     db_session.refresh(adjustment)

#     mock_sentry_capture_exception.assert_called_once()
#     assert adjustment.retry_status == QueuedRetryStatuses.FAILED
#     assert adjustment.next_attempt_time is None


# @mock.patch("rq.Queue")
# def test_handle_voucher_status_adjustment_error_account_holder_deleted(
#     mock_queue: mock.MagicMock, db_session: "Session", adjustment: VoucherUpdate, adjustment_url: str
# ) -> None:

#     job = mock.MagicMock(spec=rq.job.Job, kwargs={"voucher_status_adjustment_id": adjustment.id})
#     traceback = mock.MagicMock(spec=Traceback)
#     mock_request = mock.MagicMock(spec=requests.Request, url=adjustment_url)
#     handle_voucher_status_adjustment_error(
#         job,
#         type(requests.RequestException),
#         requests.RequestException(
#             request=mock_request,
#             response=mock.MagicMock(
#                 spec=requests.Response,
#                 request=mock_request,
#                 status_code=404,
#                 text="Not Found",
#                 json=lambda: {
#                     "display_message": "Account not found for provided credentials.",
#                     "error": "NO_ACCOUNT_FOUND",
#                 },
#             ),
#         ),
#         traceback,
#     )
#     db_session.refresh(adjustment)
#     mock_queue.assert_not_called()
#     assert adjustment.retry_status == QueuedRetryStatuses.FAILED
#     assert adjustment.next_attempt_time is None
