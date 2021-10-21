from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKeyValue

from app.core.config import settings
from app.enums import VoucherUpdateStatuses
from app.models import Voucher, VoucherUpdate
from app.models.voucher import VoucherConfig

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@pytest.fixture(scope="function")
def voucher_issuance_task_params(voucher: Voucher) -> dict:
    now = datetime.utcnow()
    return {
        "account_url": "http://test.url/",
        "voucher_id": str(voucher.id),
        "voucher_code": voucher.voucher_code,
        "issued_date": str(now.timestamp()),
        "expiry_date": str(
            (now + timedelta(days=voucher.voucher_config.validity_days)).timestamp()  # type: ignore [arg-type]
        ),
        "voucher_config_id": str(voucher.voucher_config_id),
        "voucher_type_slug": voucher.voucher_config.voucher_type_slug,
    }


@pytest.fixture(scope="function")
def voucher_issuance_task_params_no_voucher(voucher_config: VoucherConfig) -> dict:
    now = datetime.utcnow()
    return {
        "account_url": "http://test.url/",
        "issued_date": str(now.timestamp()),
        "expiry_date": str((now + timedelta(days=voucher_config.validity_days)).timestamp()),  # type: ignore [arg-type]
        "voucher_config_id": str(voucher_config.id),
        "voucher_type_slug": voucher_config.voucher_type_slug,
    }


@pytest.fixture(scope="function")
def issuance_retry_task(
    db_session: "Session", voucher_issuance_task_params: dict, voucher_issuance_task_type: TaskType
) -> RetryTask:
    task = RetryTask(task_type_id=voucher_issuance_task_type.task_type_id)
    db_session.add(task)
    db_session.flush()

    key_ids = voucher_issuance_task_type.get_key_ids_by_name()
    db_session.add_all(
        [
            TaskTypeKeyValue(
                task_type_key_id=key_ids[key],
                value=value,
                retry_task_id=task.retry_task_id,
            )
            for key, value in voucher_issuance_task_params.items()
        ]
    )
    db_session.commit()
    return task


@pytest.fixture(scope="function")
def issuance_retry_task_no_voucher(
    db_session: "Session", voucher_issuance_task_params_no_voucher: dict, voucher_issuance_task_type: TaskType
) -> RetryTask:
    task = RetryTask(task_type_id=voucher_issuance_task_type.task_type_id)
    db_session.add(task)
    db_session.flush()

    key_ids = voucher_issuance_task_type.get_key_ids_by_name()
    db_session.add_all(
        [
            TaskTypeKeyValue(
                task_type_key_id=key_ids[key],
                value=value,
                retry_task_id=task.retry_task_id,
            )
            for key, value in voucher_issuance_task_params_no_voucher.items()
        ]
    )
    db_session.commit()
    return task


@pytest.fixture(scope="function")
def issuance_expected_payload(voucher_issuance_task_params: dict) -> dict:
    return {
        "voucher_code": voucher_issuance_task_params["voucher_code"],
        "issued_date": voucher_issuance_task_params["issued_date"],
        "expiry_date": voucher_issuance_task_params["expiry_date"],
        "voucher_type_slug": voucher_issuance_task_params["voucher_type_slug"],
        "voucher_id": voucher_issuance_task_params["voucher_id"],
    }


@pytest.fixture(scope="function")
def voucher_update(db_session: "Session", voucher: Voucher) -> VoucherUpdate:
    adjustment = VoucherUpdate(
        voucher=voucher,
        date=datetime.utcnow().date(),
        status=VoucherUpdateStatuses.REDEEMED,
    )
    db_session.add(adjustment)
    db_session.commit()
    return adjustment


@pytest.fixture(scope="function")
def voucher_status_adjustment_task_params(voucher_update: VoucherUpdate) -> dict:
    return {
        "voucher_id": str(voucher_update.voucher_id),
        "retailer_slug": voucher_update.voucher.retailer_slug,
        "date": str(datetime.fromisoformat(voucher_update.date.isoformat()).timestamp()),
        "status": voucher_update.status.name,  # type: ignore [attr-defined]
    }


@pytest.fixture(scope="function")
def voucher_status_adjustment_retry_task(
    db_session: "Session", voucher_status_adjustment_task_params: dict, voucher_status_adjustment_task_type: TaskType
) -> RetryTask:
    task = RetryTask(task_type_id=voucher_status_adjustment_task_type.task_type_id)
    db_session.add(task)
    db_session.flush()

    key_ids = voucher_status_adjustment_task_type.get_key_ids_by_name()
    db_session.add_all(
        [
            TaskTypeKeyValue(
                task_type_key_id=key_ids[key],
                value=value,
                retry_task_id=task.retry_task_id,
            )
            for key, value in voucher_status_adjustment_task_params.items()
        ]
    )
    db_session.commit()
    return task


@pytest.fixture(scope="function")
def adjustment_expected_payload(voucher_status_adjustment_retry_task: RetryTask) -> dict:
    params = voucher_status_adjustment_retry_task.get_params()
    return {
        "status": params["status"],
        "date": params["date"],
    }


@pytest.fixture(scope="function")
def adjustment_url(voucher_status_adjustment_task_params: dict) -> str:
    return "{base_url}/bpl/loyalty/{retailer_slug}/vouchers/{voucher_id}/status".format(
        base_url=settings.POLARIS_URL,
        retailer_slug=voucher_status_adjustment_task_params["retailer_slug"],
        voucher_id=voucher_status_adjustment_task_params["voucher_id"],
    )
