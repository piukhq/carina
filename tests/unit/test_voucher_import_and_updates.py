import datetime
import logging

from collections import defaultdict
from typing import TYPE_CHECKING, DefaultDict, List

import pytest

from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from pytest_mock import MockerFixture
from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKeyValue
from sqlalchemy.future import select
from testfixtures import LogCapture

from app.enums import FileAgentType, VoucherUpdateStatuses
from app.imports.agents.file_agent import (
    BlobProcessingError,
    VoucherFileLog,
    VoucherImportAgent,
    VoucherUpdateRow,
    VoucherUpdatesAgent,
)
from app.models import Voucher, VoucherUpdate
from app.schemas import VoucherUpdateSchema
from app.tasks.status_adjustment import status_adjustment
from tests.api.conftest import SetupType

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _get_voucher_update_rows(db_session: "Session", voucher_codes: List[str]) -> List[VoucherUpdate]:
    voucher_updates = (
        db_session.execute(select(VoucherUpdate).join(Voucher).where(Voucher.voucher_code.in_(voucher_codes)))
        .scalars()
        .all()
    )
    return voucher_updates


def _get_voucher_rows(db_session: "Session") -> List[Voucher]:
    return db_session.execute(select(Voucher)).scalars().all()


def test_import_agent__process_csv(setup: SetupType, mocker: MockerFixture) -> None:
    mocker.patch("app.scheduler.sentry_sdk")
    db_session, voucher_config, pre_existing_voucher = setup
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk
    from app.scheduler import sentry_sdk as scheduler_sentry_sdk

    capture_message_spy = mocker.spy(scheduler_sentry_sdk, "capture_message")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.SENTRY_DSN = "SENTRY_DSN"
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    voucher_agent = VoucherImportAgent()
    eligible_voucher_codes = ["voucher1", "voucher2", "voucher3"]

    vouchers = _get_voucher_rows(db_session)
    assert len(vouchers) == 1
    assert vouchers[0] == pre_existing_voucher

    blob_content = "\n".join(eligible_voucher_codes + [pre_existing_voucher.voucher_code])
    blob_content += "\nthis,is,a,bad,line"  # this should be reported to sentry (line 5)
    blob_content += "\nanother,bad,line"  # this should be reported to sentry (line 6)

    voucher_agent.process_csv(
        retailer_slug=voucher_config.retailer_slug,
        blob_name="test-retailer/available-vouchers/test-voucher/new-vouchers.csv",
        blob_content=blob_content,
        db_session=db_session,
    )

    vouchers = _get_voucher_rows(db_session)
    assert len(vouchers) == 4
    assert all(v in [voucher.voucher_code for voucher in vouchers] for v in eligible_voucher_codes)
    # We should be sentry warned about the existing token
    assert capture_message_spy.call_count == 2  # Errors should all be rolled up in to one call per error category
    assert (
        "Invalid rows found in test-retailer/available-vouchers/test-voucher/new-vouchers.csv:\nrows: 5, 6"
        == capture_message_spy.call_args_list[0][0][0]
    )
    assert (
        "Pre-existing voucher codes found in test-retailer/available-vouchers/test-voucher/new-vouchers.csv:\nrows: 4"
        == capture_message_spy.call_args_list[1][0][0]
    )


def test_import_agent__process_csv_no_voucher_config(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, voucher_config, _ = setup

    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    voucher_agent = VoucherImportAgent()
    with pytest.raises(BlobProcessingError) as exc_info:
        voucher_agent.process_csv(
            retailer_slug=voucher_config.retailer_slug,
            blob_name="test-retailer/available-vouchers/incorrect-voucher-type/new-vouchers.csv",
            blob_content="voucher1\nvoucher2\nvoucher3",
            db_session=db_session,
        )
    assert exc_info.value.args == ("No VoucherConfig found for voucher_type_slug incorrect-voucher-type",)


def test_import_agent__process_csv_no_voucher_type_in_path(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, voucher_config, _ = setup

    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    voucher_agent = VoucherImportAgent()
    with pytest.raises(BlobProcessingError) as exc_info:
        voucher_agent.process_csv(
            retailer_slug=voucher_config.retailer_slug,
            blob_name="test-retailer/available-vouchers/new-vouchers.csv",
            blob_content="voucher1\nvoucher2\nvoucher3",
            db_session=db_session,
        )
    assert exc_info.value.args == (
        "No voucher_type_slug path section found (not enough values to unpack (expected 2, got 1))",
    )


def test_updates_agent__process_csv(setup: SetupType, mocker: MockerFixture) -> None:
    # GIVEN
    db_session, voucher_config, _ = setup

    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    voucher_agent = VoucherUpdatesAgent()
    mock__process_updates = mocker.patch.object(voucher_agent, "_process_updates")
    blob_name = "/test-retailer/voucher-updates/test.csv"
    content = """
TEST12345678,2021-07-30,cancelled
TEST87654321,2021-07-21,redeemed
TEST87654322,2021-07-30,cancelled
TEST87654322,2021-07-30,redeemed
""".strip()

    voucher_agent.process_csv(
        retailer_slug=voucher_config.retailer_slug,
        blob_name=blob_name,
        blob_content=content,
        db_session=db_session,
    )
    expected_voucher_update_rows_by_code = defaultdict(
        list[VoucherUpdateRow],
        {
            "TEST12345678": [
                VoucherUpdateRow(
                    row_num=1,
                    data=VoucherUpdateSchema(
                        voucher_code="TEST12345678", date="2021-07-30", status=VoucherUpdateStatuses.CANCELLED
                    ),
                )
            ],
            "TEST87654321": [
                VoucherUpdateRow(
                    row_num=2,
                    data=VoucherUpdateSchema(
                        voucher_code="TEST87654321", date="2021-07-21", status=VoucherUpdateStatuses.REDEEMED
                    ),
                )
            ],
            "TEST87654322": [
                VoucherUpdateRow(
                    row_num=3,
                    data=VoucherUpdateSchema(
                        voucher_code="TEST87654322", date="2021-07-30", status=VoucherUpdateStatuses.CANCELLED
                    ),
                ),
                VoucherUpdateRow(
                    row_num=4,
                    data=VoucherUpdateSchema(
                        voucher_code="TEST87654322", date="2021-07-30", status=VoucherUpdateStatuses.REDEEMED
                    ),
                ),
            ],
        },
    )
    mock__process_updates.assert_called_once_with(
        retailer_slug="test-retailer",
        blob_name=blob_name,
        db_session=db_session,
        voucher_update_rows_by_code=expected_voucher_update_rows_by_code,
    )


def test_updates_agent__process_csv_voucher_code_fails_non_validating_rows(
    setup: SetupType, mocker: MockerFixture
) -> None:
    """If non-validating values are encountered, sentry should log a msg"""
    # GIVEN
    db_session, voucher_config, voucher = setup
    voucher.allocated = True
    db_session.commit()

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch.object(VoucherUpdatesAgent, "enqueue_voucher_updates")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    bad_date = "20210830"
    bad_status = "nosuchstatus"
    content = f"""
TSTCD1234,2021-07-30,redeemed
TEST12345678,{bad_date},redeemed
TEST666666,2021-07-30,{bad_status}
""".strip()

    # WHEN
    voucher_agent.process_csv(
        retailer_slug=voucher_config.retailer_slug,
        blob_name=blob_name,
        blob_content=content,
        db_session=db_session,
    )

    # THEN
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert f"time data '{bad_date}' does not match format '%Y-%m-%d'" in expected_error_msg
    assert f"'{bad_status}' is not a valid VoucherUpdateStatuses" in expected_error_msg


def test_updates_agent__process_csv_voucher_code_fails_malformed_csv_rows(
    setup: SetupType, mocker: MockerFixture
) -> None:
    """If a bad CSV row in encountered, sentry should log a msg"""
    # GIVEN
    db_session, voucher_config, _ = setup

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch.object(VoucherUpdatesAgent, "enqueue_voucher_updates")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    content = "TEST87654321,2021-07-30\nTEST12345678,redeemed\n"

    # WHEN
    voucher_agent.process_csv(
        retailer_slug=voucher_config.retailer_slug,
        blob_name=blob_name,
        blob_content=content,
        db_session=db_session,
    )

    # THEN
    assert capture_message_spy.call_count == 1  # Both index errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert "IndexError('list index out of range')" in expected_error_msg


def test_updates_agent__process_updates(setup: SetupType, mocker: MockerFixture) -> None:
    # GIVEN
    db_session, voucher_config, voucher = setup
    voucher.allocated = True
    db_session.commit()

    mock_enqueue = mocker.patch.object(VoucherUpdatesAgent, "enqueue_voucher_updates", autospec=True)

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
    voucher_update_rows_by_code[voucher.voucher_code].append(VoucherUpdateRow(data=data, row_num=1))

    # WHEN
    voucher_agent._process_updates(
        retailer_slug=voucher_config.retailer_slug,
        voucher_update_rows_by_code=voucher_update_rows_by_code,
        blob_name=blob_name,
        db_session=db_session,
    )
    voucher_update_rows = _get_voucher_update_rows(db_session, [voucher.voucher_code])

    assert len(voucher_update_rows) == 1
    assert voucher_update_rows[0].voucher_id == voucher.id
    assert voucher_update_rows[0].status == VoucherUpdateStatuses.REDEEMED
    assert isinstance(voucher_update_rows[0].date, datetime.date)
    assert str(voucher_update_rows[0].date) == "2021-07-30"
    assert capture_message_spy.call_count == 0  # Should be no errors
    assert mock_enqueue.called


def test_updates_agent__process_updates_voucher_code_not_allocated(setup: SetupType, mocker: MockerFixture) -> None:
    """If the voucher is not allocated, it should be soft-deleted"""
    # GIVEN
    db_session, voucher_config, voucher = setup
    voucher.allocated = False
    db_session.commit()

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch.object(VoucherUpdatesAgent, "enqueue_voucher_updates")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO
    voucher_agent = VoucherUpdatesAgent()
    mocker.patch.object(voucher_agent, "_report_unknown_codes", autospec=True)
    blob_name = "/test-retailer/voucher-updates/test.csv"
    data = VoucherUpdateSchema(
        voucher_code=voucher.voucher_code,
        date="2021-07-30",
        status=VoucherUpdateStatuses("redeemed"),
    )
    voucher_update_rows_by_code: DefaultDict[str, List[VoucherUpdateRow]] = defaultdict(list[VoucherUpdateRow])
    voucher_update_rows_by_code[voucher.voucher_code].append(VoucherUpdateRow(data=data, row_num=1))

    # WHEN
    voucher_agent._process_updates(
        db_session=db_session,
        retailer_slug=voucher_config.retailer_slug,
        voucher_update_rows_by_code=voucher_update_rows_by_code,
        blob_name=blob_name,
    )
    voucher_update_rows = _get_voucher_update_rows(db_session, [voucher.voucher_code])

    # THEN
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert "Unallocated voucher codes found" in expected_error_msg
    assert len(voucher_update_rows) == 0


def test_updates_agent__process_updates_voucher_code_does_not_exist(setup: SetupType, mocker: MockerFixture) -> None:
    """The voucher does not exist"""
    # GIVEN
    db_session, voucher_config, _ = setup

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch.object(VoucherUpdatesAgent, "enqueue_voucher_updates")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO
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
    voucher_update_rows_by_code[bad_voucher_code].append(VoucherUpdateRow(data=data, row_num=1))

    # WHEN
    voucher_agent._process_updates(
        retailer_slug=voucher_config.retailer_slug,
        voucher_update_rows_by_code=voucher_update_rows_by_code,
        blob_name=blob_name,
        db_session=db_session,
    )
    voucher_update_rows = _get_voucher_update_rows(db_session, [bad_voucher_code])

    # THEN
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert (
        "Unknown voucher codes found while processing /test-retailer/voucher-updates/test.csv, rows: 1"
        in expected_error_msg
    )
    assert len(voucher_update_rows) == 0


class Blob:
    def __init__(self, name: str):
        self.name = name


def test_process_blobs(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, _, _ = setup
    MockBlobServiceClient = mocker.patch("app.imports.agents.file_agent.BlobServiceClient", autospec=True)
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)

    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    voucher_agent = VoucherUpdatesAgent()
    container_client = mocker.patch.object(voucher_agent, "container_client", spec=ContainerClient)
    mock_process_csv = mocker.patch.object(voucher_agent, "process_csv")
    mock_move_blob = mocker.patch.object(voucher_agent, "move_blob")
    file_name = "test-retailer/voucher-updates/update.csv"
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(file_name),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value = mocker.MagicMock(spec=BlobClient)

    voucher_agent.process_blobs("test-retailer", db_session=db_session)

    voucher_file_log_row = db_session.execute(
        select(VoucherFileLog).where(
            VoucherFileLog.file_name == file_name, VoucherFileLog.file_agent_type == FileAgentType.UPDATE
        )
    ).scalar_one_or_none()

    assert voucher_file_log_row
    mock_process_csv.assert_called_once()
    mock_move_blob.assert_called_once()


def test_process_blobs_unicodedecodeerror(capture: LogCapture, setup: SetupType, mocker: MockerFixture) -> None:
    db_session, _, _ = setup
    MockBlobServiceClient = mocker.patch("app.imports.agents.file_agent.BlobServiceClient", autospec=True)
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)

    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    voucher_agent = VoucherUpdatesAgent()
    container_client = mocker.patch.object(voucher_agent, "container_client", spec=ContainerClient)
    mock_process_csv = mocker.patch.object(voucher_agent, "process_csv")
    mock_move_blob = mocker.patch.object(voucher_agent, "move_blob")
    blob_filename = "test-retailer/voucher-updates/update.csv"
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(blob_filename),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value.download_blob.return_value.readall.return_value = (
        b"\xca,2021,09,13,cancelled"
    )

    voucher_agent.process_blobs("test-retailer", db_session=db_session)

    assert not mock_process_csv.called
    message = f"Problem decoding blob {blob_filename} (files should be utf-8 encoded)"
    assert any(message in record.msg for record in capture.records)
    mock_move_blob.assert_called_once()


def test_process_blobs_not_csv(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, _, _ = setup
    MockBlobServiceClient = mocker.patch("app.imports.agents.file_agent.BlobServiceClient", autospec=True)
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)
    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    from app.imports.agents.file_agent import sentry_sdk

    capture_message_spy = mocker.spy(sentry_sdk, "capture_message")

    voucher_agent = VoucherUpdatesAgent()
    mock_process_csv = mocker.patch.object(voucher_agent, "process_csv")
    container_client = mocker.patch.object(voucher_agent, "container_client", spec=ContainerClient)
    mock_move_blob = mocker.patch.object(voucher_agent, "move_blob")
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob("test-retailer/voucher-updates/update.docx"),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value = mocker.MagicMock(spec=BlobClient)
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.BLOB_ERROR_CONTAINER = "ERROR-CONTAINER"

    voucher_agent.process_blobs("test-retailer", db_session=db_session)
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    assert (
        "test-retailer/voucher-updates/update.docx does not have .csv ext. Moving to ERROR-CONTAINER for checking"
        == capture_message_spy.call_args.args[0]
    )
    mock_move_blob.assert_called_once()
    assert mock_move_blob.call_args[0][0] == "ERROR-CONTAINER"
    mock_process_csv.assert_not_called()


def test_process_blobs_filename_is_duplicate(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, _, _ = setup
    file_name = "test-retailer/voucher-updates/update.csv"
    db_session.add(
        VoucherFileLog(
            file_name=file_name,
            file_agent_type=FileAgentType.UPDATE,
        )
    )
    db_session.commit()
    MockBlobServiceClient = mocker.patch("app.imports.agents.file_agent.BlobServiceClient", autospec=True)
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)
    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    from app.imports.agents.file_agent import sentry_sdk

    capture_message_spy = mocker.spy(sentry_sdk, "capture_message")

    voucher_agent = VoucherUpdatesAgent()
    mock_process_csv = mocker.patch.object(voucher_agent, "process_csv")
    container_client = mocker.patch.object(voucher_agent, "container_client", spec=ContainerClient)
    mock_move_blob = mocker.patch.object(voucher_agent, "move_blob")
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(file_name),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value = mocker.MagicMock(spec=BlobClient)
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.BLOB_ERROR_CONTAINER = "ERROR-CONTAINER"

    voucher_agent.process_blobs("test-retailer", db_session=db_session)
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    assert (
        "test-retailer/voucher-updates/update.csv is a duplicate. Moving to ERROR-CONTAINER for checking"
        == capture_message_spy.call_args.args[0]
    )
    mock_move_blob.assert_called_once()
    assert mock_move_blob.call_args[0][0] == "ERROR-CONTAINER"
    mock_process_csv.assert_not_called()


def test_process_blobs_filename_is_not_duplicate(setup: SetupType, mocker: MockerFixture) -> None:
    """A filename exists in the log, but the file agent type is different"""
    db_session, _, _ = setup
    file_name = "test-retailer/voucher-updates/update.csv"
    db_session.add(
        VoucherFileLog(
            file_name=file_name,
            file_agent_type=FileAgentType.IMPORT,
        )
    )
    db_session.commit()
    MockBlobServiceClient = mocker.patch("app.imports.agents.file_agent.BlobServiceClient", autospec=True)
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)

    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    voucher_agent = VoucherUpdatesAgent()
    container_client = mocker.patch.object(voucher_agent, "container_client", spec=ContainerClient)
    mock_process_csv = mocker.patch.object(voucher_agent, "process_csv")
    mock_move_blob = mocker.patch.object(voucher_agent, "move_blob")
    file_name = "test-retailer/voucher-updates/update.csv"
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(file_name),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value = mocker.MagicMock(spec=BlobClient)

    voucher_agent.process_blobs("test-retailer", db_session=db_session)

    voucher_file_log_row = db_session.execute(
        select(VoucherFileLog).where(
            VoucherFileLog.file_name == file_name, VoucherFileLog.file_agent_type == FileAgentType.UPDATE
        )
    ).scalar_one_or_none()

    assert voucher_file_log_row  # The new record
    mock_process_csv.assert_called_once()
    mock_move_blob.assert_called_once()


def test_move_blob(mocker: MockerFixture) -> None:
    # GIVEN
    MockBlobServiceClient = mocker.patch("app.imports.agents.file_agent.BlobServiceClient", autospec=True)
    MockBlobServiceClient.from_connection_string.return_value = mocker.MagicMock(spec=BlobServiceClient)
    mock_src_blob_client = mocker.patch("app.imports.agents.file_agent.BlobClient", autospec=True)
    mock_src_blob_client.url = "https://a-blob-url"
    src_blob_lease_client = mocker.patch("app.imports.agents.file_agent.BlobLeaseClient", autospec=True)

    voucher_agent = VoucherUpdatesAgent()
    mock_src_blob_client.blob_name = blob_name = "/test-retailer/voucher-updates/test.csv"

    blob_service_client = mocker.patch.object(voucher_agent, "blob_service_client")
    mock_dst_blob_client = mocker.MagicMock()
    blob_service_client.delete_blob = mocker.MagicMock()
    blob_service_client.get_blob_client.return_value = mock_dst_blob_client

    # WHEN
    voucher_agent.move_blob(
        "DESTINATION-CONTAINER",
        mock_src_blob_client,
        src_blob_lease_client,
    )
    blob = f"{datetime.datetime.utcnow().strftime('%Y/%m/%d/%H%M')}/{blob_name}"

    # THEN
    blob_service_client.get_blob_client.assert_called_once_with("DESTINATION-CONTAINER", blob)
    mock_dst_blob_client.start_copy_from_url.assert_called_once_with("https://a-blob-url")
    mock_src_blob_client.delete_blob.assert_called_once()


def test_enqueue_voucher_updates(
    setup: SetupType, mocker: MockerFixture, voucher_status_adjustment_task_type: TaskType
) -> None:
    db_session, _, voucher = setup

    mock_queue = mocker.patch("rq.Queue")
    voucher_update = VoucherUpdate(
        voucher=voucher,
        date=datetime.datetime.utcnow().date(),
        status=VoucherUpdateStatuses.REDEEMED,
    )
    db_session.add(voucher_update)
    db_session.commit()

    VoucherUpdatesAgent.enqueue_voucher_updates(db_session, [voucher_update])
    mock_queue.return_value.enqueue_many.assert_called_once()
    mock_queue.prepare_data.assert_called_once()

    retry_task = (
        db_session.execute(
            select(RetryTask).where(
                RetryTask.task_type_id == voucher_status_adjustment_task_type.task_type_id,
                RetryTask.retry_task_id == TaskTypeKeyValue.retry_task_id,
                TaskTypeKeyValue.value == str(voucher_update.voucher.id),
            )
        )
        .scalars()
        .first()
    )

    mock_queue.prepare_data.assert_called_with(
        status_adjustment,
        kwargs={"retry_task_id": retry_task.retry_task_id},
        failure_ttl=60 * 60 * 24 * 7,
    )
