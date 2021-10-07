from fastapi import status
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture

from app.core.config import settings
from app.enums import VoucherTypeStatuses
from app.models import VoucherAllocation
from asgi import app
from tests.api.conftest import SetupType
from tests.fixtures import HttpErrors

client = TestClient(app)
auth_headers = {"Authorization": f"token {settings.CARINA_AUTH_TOKEN}"}
payload = {"account_url": "http://test.url/"}


def test_post_voucher_allocation_happy_path(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, voucher_config, voucher = setup
    mocker.patch("app.tasks.voucher.rq.Queue")

    resp = client.post(
        f"/bpl/vouchers/{voucher_config.retailer_slug}/vouchers/{voucher_config.voucher_type_slug}/allocation",
        json=payload,
        headers=auth_headers,
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}

    voucher_allocation = db_session.query(VoucherAllocation).filter_by(voucher_id=voucher.id).first()

    assert voucher_allocation is not None
    assert voucher_allocation.voucher_config == voucher_config
    assert voucher_allocation.account_url == payload["account_url"]


def test_post_voucher_allocation_wrong_retailer(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, voucher_config, voucher = setup

    resp = client.post(
        f"/bpl/vouchers/WRONG-RETAILER/vouchers/{voucher_config.voucher_type_slug}/allocation",
        json=payload,
        headers=auth_headers,
    )

    assert resp.status_code == HttpErrors.INVALID_RETAILER.value.status_code
    assert resp.json() == HttpErrors.INVALID_RETAILER.value.detail

    voucher_allocation = db_session.query(VoucherAllocation).filter_by(voucher_id=voucher.id).first()
    assert voucher_allocation is None


def test_post_voucher_allocation_wrong_voucher_type(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, voucher_config, voucher = setup

    resp = client.post(
        f"/bpl/vouchers/{voucher_config.retailer_slug}/vouchers/WRONG-TYPE/allocation",
        json=payload,
        headers=auth_headers,
    )

    assert resp.status_code == HttpErrors.UNKNOWN_VOUCHER_TYPE.value.status_code
    assert resp.json() == HttpErrors.UNKNOWN_VOUCHER_TYPE.value.detail

    voucher_allocation = db_session.query(VoucherAllocation).filter_by(voucher_id=voucher.id).first()
    assert voucher_allocation is None


def test_post_voucher_allocation_no_more_vouchers(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, voucher_config, voucher = setup
    voucher.allocated = True
    db_session.commit()

    mocker.patch("app.tasks.voucher.rq.Queue")

    resp = client.post(
        f"/bpl/vouchers/{voucher_config.retailer_slug}/vouchers/{voucher_config.voucher_type_slug}/allocation",
        json=payload,
        headers=auth_headers,
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}

    voucher_allocation = db_session.query(VoucherAllocation).filter_by(voucher_config_id=voucher_config.id).first()

    assert voucher_allocation is not None
    assert voucher_allocation.voucher_id is None
    assert voucher_allocation.account_url == payload["account_url"]


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
