import datetime

from functools import partial

from pytest_mock import MockerFixture
from sqlalchemy.orm import Session

from app.core.config import settings
from app.enums import VoucherUpdateStatuses
from app.imports.agents.file_agent import VoucherUpdatesAgent
from app.models import VoucherConfig, VoucherUpdate
from tests.api.conftest import SetupType


def test_process_csv(setup: SetupType, mocker: MockerFixture) -> None:
    # GIVEN
    db_session, voucher_config, voucher = setup
    voucher.allocated = True
    db_session.commit()

    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    content = f"{voucher.voucher_code},2021-07-30,redeemed\n"
    byte_content = content.encode("utf-8")

    # WHEN
    voucher_agent.process_csv(
        db_session=db_session,
        retailer_slug=voucher_config.retailer_slug,
        blob_name=blob_name,
        byte_content=byte_content,
        voucher_config_id=voucher_config.id,
    )
    voucher_update_rows = db_session.query(VoucherUpdate).filter_by(voucher_config_id=voucher_config.id).all()

    # THEN
    assert len(voucher_update_rows) == 1
    assert voucher_update_rows[0].voucher_code == f"{voucher.voucher_code}"
    assert voucher_update_rows[0].status == VoucherUpdateStatuses.REDEEMED
    assert isinstance(voucher_update_rows[0].date, datetime.date)
    assert str(voucher_update_rows[0].date) == "2021-07-30"


def test_process_csv_voucher_code_not_allocated(setup: SetupType, mocker: MockerFixture) -> None:
    """If the voucher is not allocated, it should be soft-deleted"""
    # GIVEN
    db_session, voucher_config, voucher = setup
    voucher.allocated = False
    db_session.commit()

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch("app.imports.agents.file_agent.settings")
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    content = f"{voucher.voucher_code},2021-07-30,redeemed\n"
    byte_content = content.encode("utf-8")

    # WHEN
    voucher_agent.process_csv(
        db_session=db_session,
        retailer_slug=voucher_config.retailer_slug,
        blob_name=blob_name,
        byte_content=byte_content,
        voucher_config_id=voucher_config.id,
    )
    voucher_update_rows = db_session.query(VoucherUpdate).filter_by(voucher_config_id=voucher_config.id).all()

    # THEN
    assert len(voucher_update_rows) == 0
    assert not voucher.allocated
    assert voucher.deleted
    assert capture_message_spy.call_count == 1


def test_process_csv_voucher_code_does_not_exist(
    db_session: Session, voucher_config: VoucherConfig, mocker: MockerFixture
) -> None:
    """The voucher does not exist"""
    # GIVEN
    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch("app.imports.agents.file_agent.settings")
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    byte_content = b"IDONOTEXIST,2021-07-30,cancelled\n"

    # WHEN
    voucher_agent.process_csv(
        db_session=db_session,
        retailer_slug=voucher_config.retailer_slug,
        blob_name=blob_name,
        byte_content=byte_content,
        voucher_config_id=voucher_config.id,
    )
    voucher_update_rows = db_session.query(VoucherUpdate).filter_by(voucher_config_id=voucher_config.id).all()

    # THEN
    assert len(voucher_update_rows) == 0
    assert capture_message_spy.call_count == 1


def test_process_csv_voucher_code_bad_csv_row_gets_reported(
    db_session: Session, voucher_config: VoucherConfig, mocker: MockerFixture
) -> None:
    """A bad CSV row gets reported to sentry"""
    # GIVEN
    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch("app.imports.agents.file_agent.settings")
    voucher_updates_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    byte_content = b"pmPgdleIAI5wVQR,2021-08-20,not_an_enum_value\n"

    # WHEN
    voucher_updates_agent.process_csv(
        db_session=db_session,
        retailer_slug=voucher_config.retailer_slug,
        blob_name=blob_name,
        byte_content=byte_content,
        voucher_config_id=voucher_config.id,
    )
    voucher_update_rows = db_session.query(VoucherUpdate).filter_by(voucher_config_id=voucher_config.id).all()

    # THEN
    assert len(voucher_update_rows) == 0
    assert capture_message_spy.call_count == 1


def test_archive(mocker: MockerFixture) -> None:
    # GIVEN
    blob_service_client = mocker.patch("app.imports.agents.file_agent.BlobServiceClient", autospec=True)
    blob_service_client.delete_blob = mocker.MagicMock
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    byte_content = b"A2eYRbcvF9yYpW2,2021-07-30,redeemed\n08oVk3czC9QYLBJ,2021-08-31,cancelled\n"

    # WHEN
    voucher_agent.archive(
        blob_name,
        byte_content,
        delete_callback=partial(blob_service_client.delete_blob, lease=mocker.MagicMock),
        blob_service_client=blob_service_client,
        logger=mocker.MagicMock,
    )
    blob = f"{datetime.datetime.now().strftime('%Y/%m/%d')}/{blob_name}"

    # THEN
    blob_service_client.get_blob_client.assert_called_once_with(settings.BLOB_ARCHIVE_CONTAINER, blob)
    blob_service_client.get_blob_client.return_value.upload_blob.assert_called_once_with(byte_content)
