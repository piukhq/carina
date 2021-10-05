from typing import TYPE_CHECKING, List, Tuple

from fastapi import status
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from retry_task_lib.db.models import RetryTask, TaskType, TaskTypeKeyValue
from sqlalchemy.future import select

from app.core.config import settings
from app.enums import VoucherTypeStatuses
from asgi import app
from tests.api.conftest import SetupType
from tests.fixtures import HttpErrors

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


client = TestClient(app)
auth_headers = {"Authorization": f"token {settings.CARINA_AUTH_TOKEN}"}
payload = {"account_url": "http://test.url/"}


def _get_retry_task_and_values(
    db_session: "Session", task_type_id: int, voucher_config_id: int
) -> Tuple[RetryTask, List[str]]:
    values: List[str] = []
    retry_task: RetryTask = (
        db_session.execute(
            select(RetryTask).where(
                RetryTask.task_type_id == task_type_id,
                RetryTask.retry_task_id == TaskTypeKeyValue.retry_task_id,
                TaskTypeKeyValue.value == str(voucher_config_id),
            )
        )
        .scalars()
        .first()
    )
    if retry_task:
        values = [value.value for value in retry_task.task_type_key_values]

    return retry_task, values


def test_post_voucher_allocation_happy_path(
    setup: SetupType, mocker: MockerFixture, voucher_issuance_task_type: TaskType
) -> None:
    db_session, voucher_config, voucher = setup
    mocker.patch("app.tasks.voucher.enqueue_retry_task")

    resp = client.post(
        f"/bpl/vouchers/{voucher_config.retailer_slug}/vouchers/{voucher_config.voucher_type_slug}/allocation",
        json=payload,
        headers=auth_headers,
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}

    retry_task, task_params_values = _get_retry_task_and_values(
        db_session, voucher_issuance_task_type.task_type_id, voucher_config.id
    )

    assert retry_task is not None
    assert payload["account_url"] in task_params_values
    assert str(voucher_config.id) in task_params_values
    assert str(voucher.id) in task_params_values


def test_post_voucher_allocation_wrong_retailer(setup: SetupType, voucher_issuance_task_type: TaskType) -> None:
    db_session, voucher_config, _ = setup

    resp = client.post(
        f"/bpl/vouchers/WRONG-RETAILER/vouchers/{voucher_config.voucher_type_slug}/allocation",
        json=payload,
        headers=auth_headers,
    )

    assert resp.status_code == HttpErrors.INVALID_RETAILER.value.status_code
    assert resp.json() == HttpErrors.INVALID_RETAILER.value.detail

    retry_task, _ = _get_retry_task_and_values(db_session, voucher_issuance_task_type.task_type_id, voucher_config.id)
    assert retry_task is None


def test_post_voucher_allocation_wrong_voucher_type(setup: SetupType, voucher_issuance_task_type: TaskType) -> None:
    db_session, voucher_config, _ = setup

    resp = client.post(
        f"/bpl/vouchers/{voucher_config.retailer_slug}/vouchers/WRONG-TYPE/allocation",
        json=payload,
        headers=auth_headers,
    )

    assert resp.status_code == HttpErrors.UNKNOWN_VOUCHER_TYPE.value.status_code
    assert resp.json() == HttpErrors.UNKNOWN_VOUCHER_TYPE.value.detail

    retry_task, _ = _get_retry_task_and_values(db_session, voucher_issuance_task_type.task_type_id, voucher_config.id)
    assert retry_task is None


def test_post_voucher_allocation_no_more_vouchers(
    setup: SetupType, mocker: MockerFixture, voucher_issuance_task_type: TaskType
) -> None:
    db_session, voucher_config, voucher = setup
    voucher.allocated = True
    db_session.commit()

    mocker.patch("app.tasks.voucher.enqueue_retry_task")

    resp = client.post(
        f"/bpl/vouchers/{voucher_config.retailer_slug}/vouchers/{voucher_config.voucher_type_slug}/allocation",
        json=payload,
        headers=auth_headers,
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}

    retry_task, task_params_values = _get_retry_task_and_values(
        db_session, voucher_issuance_task_type.task_type_id, voucher_config.id
    )

    assert retry_task is not None
    assert payload["account_url"] in task_params_values
    assert str(voucher_config.id) in task_params_values


def test_voucher_type_status_ok(setup: SetupType) -> None:
    db_session, voucher_config, _ = setup

    for transition_status in ("cancelled", "ended"):
        resp = client.patch(
            f"/bpl/vouchers/{voucher_config.retailer_slug}/vouchers/{voucher_config.voucher_type_slug}/status",
            json={"status": transition_status},
            headers=auth_headers,
        )
        assert resp.status_code == status.HTTP_202_ACCEPTED
        assert resp.json() == {}
        db_session.refresh(voucher_config)
        assert voucher_config.status == VoucherTypeStatuses(transition_status)


def test_voucher_type_status_bad_status(setup: SetupType) -> None:
    db_session, voucher_config, _ = setup

    resp = client.patch(
        f"/bpl/vouchers/{voucher_config.retailer_slug}/vouchers/{voucher_config.voucher_type_slug}/status",
        json={"status": "active"},
        headers=auth_headers,
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    db_session.refresh(voucher_config)
    assert voucher_config.status == VoucherTypeStatuses.ACTIVE


def test_voucher_type_status_invalid_retailer(setup: SetupType) -> None:
    db_session, voucher_config, _ = setup

    resp = client.patch(
        f"/bpl/vouchers/unknown-retailer/vouchers/{voucher_config.voucher_type_slug}/status",
        json={"status": "cancelled"},
        headers=auth_headers,
    )
    assert resp.status_code == HttpErrors.INVALID_RETAILER.value.status_code
    assert resp.json() == HttpErrors.INVALID_RETAILER.value.detail
    db_session.refresh(voucher_config)
    assert voucher_config.status == VoucherTypeStatuses.ACTIVE


def test_voucher_type_status_voucher_type_not_found(setup: SetupType) -> None:
    db_session, voucher_config, _ = setup

    resp = client.patch(
        f"/bpl/vouchers/{voucher_config.retailer_slug}/vouchers/invalid-voucher-type/status",
        json={"status": "cancelled"},
        headers=auth_headers,
    )
    assert resp.status_code == HttpErrors.UNKNOWN_VOUCHER_TYPE.value.status_code
    assert resp.json() == HttpErrors.UNKNOWN_VOUCHER_TYPE.value.detail
    db_session.refresh(voucher_config)
    assert voucher_config.status == VoucherTypeStatuses.ACTIVE
