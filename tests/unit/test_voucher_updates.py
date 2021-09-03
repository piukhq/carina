import datetime

from collections import defaultdict
from functools import partial
from typing import TYPE_CHECKING, DefaultDict, List

from pytest_mock import MockerFixture
from sqlalchemy.future import select

from app.core.config import settings
from app.enums import VoucherUpdateStatuses
from app.imports.agents.file_agent import VoucherUpdateRow, VoucherUpdatesAgent
from app.models import VoucherConfig, VoucherUpdate
from app.schemas import VoucherUpdateSchema
from tests.api.conftest import SetupType

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _get_voucher_update_rows(db_session: "Session", voucher_config: VoucherConfig) -> List[VoucherUpdate]:
    voucher_update_rows = (
        db_session.execute(select(VoucherUpdate).where(VoucherUpdate.retailer_slug == voucher_config.retailer_slug))
        .scalars()
        .all()
    )
    return voucher_update_rows


def test_process_csv(setup: SetupType, mocker: MockerFixture) -> None:
    # GIVEN
    _, voucher_config, _ = setup

    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    content = (
        "TEST12345678,2021-07-30,cancelled,2021-07-30,redeemed\nTEST87654321,2021-07-21,redeemed\n"
        "TEST87654321,2021-07-30,cancelled\n"
    )
    byte_content = content.encode("utf-8")

    # WHEN
    voucher_update_rows_by_code: DefaultDict[str, List[VoucherUpdateRow]] = voucher_agent.process_csv(
        retailer_slug=voucher_config.retailer_slug,
        blob_name=blob_name,
        byte_content=byte_content,
    )

    # THEN
    assert isinstance(voucher_update_rows_by_code["TEST12345678"][0], VoucherUpdateRow)
    assert isinstance(voucher_update_rows_by_code["TEST12345678"][0].row_num, int)
    assert isinstance(voucher_update_rows_by_code["TEST12345678"][0].voucher_update, VoucherUpdate)
    assert len(voucher_update_rows_by_code["TEST87654321"]) == 2
    assert isinstance(voucher_update_rows_by_code["TEST87654321"][1], VoucherUpdateRow)
    assert isinstance(voucher_update_rows_by_code["TEST87654321"][1].row_num, int)
    assert isinstance(voucher_update_rows_by_code["TEST87654321"][1].voucher_update, VoucherUpdate)


def test_process_csv_voucher_code_fails_non_validating_rows(setup: SetupType, mocker: MockerFixture) -> None:
    """If non-validating values are encountered, sentry should log a msg"""
    # GIVEN
    _, voucher_config, _ = setup

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch("app.imports.agents.file_agent.settings")
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    bad_date = "20210830"
    bad_status = "nosuchstatus"
    content = (
        f"TEST87654321,2021-07-30,redeemed\nTEST12345678,{bad_date},redeemed\nTEST666666,2021-07-30,{bad_status}\n"
    )
    byte_content = content.encode("utf-8")

    # WHEN
    voucher_agent.process_csv(
        retailer_slug=voucher_config.retailer_slug,
        blob_name=blob_name,
        byte_content=byte_content,
    )

    # THEN
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert f"time data '{bad_date}' does not match format '%Y-%m-%d'" in expected_error_msg
    assert f"'{bad_status}' is not a valid VoucherUpdateStatuses" in expected_error_msg


def test_process_csv_voucher_code_fails_malformed_csv_rows(setup: SetupType, mocker: MockerFixture) -> None:
    """If a bad CSV row in encountered, sentry should log a msg"""
    # GIVEN
    _, voucher_config, _ = setup

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch("app.imports.agents.file_agent.settings")
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    byte_content = b"TEST87654321,2021-07-30\nTEST12345678,redeemed\n"

    # WHEN
    voucher_agent.process_csv(
        retailer_slug=voucher_config.retailer_slug,
        blob_name=blob_name,
        byte_content=byte_content,
    )

    # THEN
    assert capture_message_spy.call_count == 1  # Both index errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert "IndexError('list index out of range')" in expected_error_msg


def test_process_updates(setup: SetupType, mocker: MockerFixture) -> None:
    # GIVEN
    db_session, voucher_config, voucher = setup
    voucher.allocated = True
    db_session.commit()

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    data = VoucherUpdateSchema(
        voucher_code=voucher.voucher_code,
        date="2021-07-30",
        status=VoucherUpdateStatuses("redeemed"),
    )
    voucher_update_rows_by_code: DefaultDict[str, List[VoucherUpdateRow]] = defaultdict(list[VoucherUpdateRow])
    voucher_update_rows_by_code[voucher.voucher_code].append(
        VoucherUpdateRow(VoucherUpdate(retailer_slug=voucher_config.retailer_slug, **data.dict()), row_num=1)
    )

    # WHEN
    voucher_agent.process_updates(
        db_session=db_session,
        retailer_slug=voucher_config.retailer_slug,
        voucher_update_rows_by_code=voucher_update_rows_by_code,
        blob_name=blob_name,
    )
    voucher_update_rows = _get_voucher_update_rows(db_session, voucher_config)

    # THEN
    assert len(voucher_update_rows) == 1
    assert voucher_update_rows[0].voucher_code == f"{voucher.voucher_code}"
    assert voucher_update_rows[0].status == VoucherUpdateStatuses.REDEEMED
    assert isinstance(voucher_update_rows[0].date, datetime.date)
    assert str(voucher_update_rows[0].date) == "2021-07-30"
    assert capture_message_spy.call_count == 0  # Should be no errors


def test_process_updates_voucher_code_not_allocated(setup: SetupType, mocker: MockerFixture) -> None:
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
    mocker.patch.object(voucher_agent, "_report_unknown_codes", autospec=True)
    blob_name = "/test-retailer/voucher-updates/test.csv"
    data = VoucherUpdateSchema(
        voucher_code=voucher.voucher_code,
        date="2021-07-30",
        status=VoucherUpdateStatuses("redeemed"),
    )
    voucher_update_rows_by_code: DefaultDict[str, List[VoucherUpdateRow]] = defaultdict(list[VoucherUpdateRow])
    voucher_update_rows_by_code[voucher.voucher_code].append(
        VoucherUpdateRow(VoucherUpdate(retailer_slug=voucher_config.retailer_slug, **data.dict()), row_num=1)
    )

    # WHEN
    voucher_agent.process_updates(
        db_session=db_session,
        retailer_slug=voucher_config.retailer_slug,
        voucher_update_rows_by_code=voucher_update_rows_by_code,
        blob_name=blob_name,
    )
    voucher_update_rows = _get_voucher_update_rows(db_session, voucher_config)

    # THEN
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert "contains unallocated Voucher codes" in expected_error_msg
    assert len(voucher_update_rows) == 0


def test_process_updates_voucher_code_does_not_exist(setup: SetupType, mocker: MockerFixture) -> None:
    """The voucher does not exist"""
    # GIVEN
    db_session, voucher_config, _ = setup

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch("app.imports.agents.file_agent.settings")
    voucher_agent = VoucherUpdatesAgent()
    mocker.patch.object(voucher_agent, "_process_unallocated_codes", autospec=True)
    blob_name = "/test-retailer/voucher-updates/test.csv"
    bad_voucher_code = "IDONOTEXIST"
    data = VoucherUpdateSchema(
        voucher_code=bad_voucher_code,
        date="2021-07-30",
        status=VoucherUpdateStatuses("cancelled"),
    )
    voucher_update_rows_by_code: DefaultDict[str, List[VoucherUpdateRow]] = defaultdict(list[VoucherUpdateRow])
    voucher_update_rows_by_code[bad_voucher_code].append(
        VoucherUpdateRow(VoucherUpdate(retailer_slug=voucher_config.retailer_slug, **data.dict()), row_num=1)
    )

    # WHEN
    voucher_agent.process_updates(
        db_session=db_session,
        retailer_slug=voucher_config.retailer_slug,
        voucher_update_rows_by_code=voucher_update_rows_by_code,
        blob_name=blob_name,
    )
    voucher_update_rows = _get_voucher_update_rows(db_session, voucher_config)

    # THEN
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert (
        "Voucher Codes Not Found while processing /test-retailer/voucher-updates/test.csv, rows: 1"
        in expected_error_msg
    )
    assert len(voucher_update_rows) == 0


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
