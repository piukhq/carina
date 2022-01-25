import datetime
import logging

from collections import defaultdict
from typing import TYPE_CHECKING, Callable, DefaultDict, List
from unittest import mock

import pytest
import redis

from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from pytest_mock import MockerFixture
from retry_tasks_lib.db.models import RetryTask, TaskType
from sqlalchemy import func
from sqlalchemy.future import select
from testfixtures import LogCapture

from app.enums import FileAgentType, RewardUpdateStatuses
from app.imports.agents.file_agent import (
    BlobProcessingError,
    RewardFileLog,
    RewardImportAgent,
    RewardUpdateRow,
    RewardUpdatesAgent,
)
from app.models import Reward, RewardUpdate
from app.schemas import RewardUpdateSchema
from tests.conftest import SetupType

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _get_reward_update_rows(db_session: "Session", reward_codes: List[str]) -> List[RewardUpdate]:
    reward_updates = (
        db_session.execute(select(RewardUpdate).join(Reward).where(Reward.code.in_(reward_codes))).scalars().all()
    )
    return reward_updates


def _get_reward_rows(db_session: "Session") -> List[Reward]:
    return db_session.execute(select(Reward)).scalars().all()


def test_import_agent__process_csv(setup: SetupType, mocker: MockerFixture) -> None:
    mocker.patch("app.scheduler.sentry_sdk")
    db_session, reward_config, pre_existing_reward = setup
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk
    from app.scheduler import sentry_sdk as scheduler_sentry_sdk

    capture_message_spy = mocker.spy(scheduler_sentry_sdk, "capture_message")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.SENTRY_DSN = "SENTRY_DSN"
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    reward_agent = RewardImportAgent()
    eligible_reward_codes = ["reward1", "reward2", "reward3"]

    rewards = _get_reward_rows(db_session)
    assert len(rewards) == 1
    assert rewards[0] == pre_existing_reward

    blob_content = "\n".join(eligible_reward_codes + [pre_existing_reward.code])
    blob_content += "\nthis,is,a,bad,line"  # this should be reported to sentry (line 5)
    blob_content += "\nanother,bad,line"  # this should be reported to sentry (line 6)

    reward_agent.process_csv(
        retailer_slug=reward_config.retailer_slug,
        blob_name="test-retailer/available-rewards/test-reward/new-reward.csv",
        blob_content=blob_content,
        db_session=db_session,
    )

    rewards = _get_reward_rows(db_session)
    assert len(rewards) == 4
    assert all(v in [reward.code for reward in rewards] for v in eligible_reward_codes)
    # We should be sentry warned about the existing token
    assert capture_message_spy.call_count == 2  # Errors should all be rolled up in to one call per error category
    assert (
        "Invalid rows found in test-retailer/available-rewards/test-reward/new-reward.csv:\nrows: 5, 6"
        == capture_message_spy.call_args_list[0][0][0]
    )
    assert (
        "Pre-existing reward codes found in test-retailer/available-rewards/test-reward/new-reward.csv:\nrows: 4"
        == capture_message_spy.call_args_list[1][0][0]
    )


def test_import_agent__process_csv_soft_deleted(
    setup: SetupType, create_reward_config: Callable, mocker: MockerFixture
) -> None:
    """
    Test that a reward code will be imported OK when the code exists in the DB but for a different reward slug,
    and it has been soft deleted
    """
    mocker.patch("app.scheduler.sentry_sdk")
    db_session, reward_config, pre_existing_reward = setup
    second_reward_config = create_reward_config(**{"reward_slug": "second-test-reward"})
    # Associate the existing reward with a different reward config i.e. a different reward slug.
    # This means the same reward code should import OK for the 'test-reward' reward slug
    pre_existing_reward.reward_config_id = second_reward_config.id
    pre_existing_reward.deleted = True
    db_session.commit()
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk
    from app.scheduler import sentry_sdk as scheduler_sentry_sdk

    mocker.spy(scheduler_sentry_sdk, "capture_message")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.SENTRY_DSN = "SENTRY_DSN"
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    reward_agent = RewardImportAgent()
    eligible_reward_codes = ["reward1", "reward2", "reward3"]

    blob_content = "\n".join(eligible_reward_codes + [pre_existing_reward.code])

    reward_agent.process_csv(
        retailer_slug=reward_config.retailer_slug,
        blob_name="test-retailer/available-rewards/test-reward/new-reward.csv",
        blob_content=blob_content,
        db_session=db_session,
    )

    rewards = _get_reward_rows(db_session)
    assert len(rewards) == 5
    assert all(v in [reward.code for reward in rewards] for v in eligible_reward_codes)
    assert capture_message_spy.call_count == 0  # All rewards should import OK


def test_import_agent__process_csv_not_soft_deleted(
    setup: SetupType, create_reward_config: Callable, mocker: MockerFixture
) -> None:
    """
    Test that a reward code imported for a different reward slug, but where that existing reward code has
    NOT been soft-deleted, will cause an error to be reported and will not be imported
    """
    mocker.patch("app.scheduler.sentry_sdk")
    db_session, reward_config, pre_existing_reward = setup
    second_reward_config = create_reward_config(**{"reward_slug": "second-test-reward"})
    # Associate the existing reward with a different reward config i.e. a different reward slug.
    # This means the same reward code should import OK for the 'test-reward' reward type slug
    pre_existing_reward.reward_config_id = second_reward_config.id
    db_session.commit()
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk
    from app.scheduler import sentry_sdk as scheduler_sentry_sdk

    mocker.spy(scheduler_sentry_sdk, "capture_message")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.SENTRY_DSN = "SENTRY_DSN"
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    reward_agent = RewardImportAgent()
    eligible_reward_codes = ["reward1", "reward2", "reward3"]

    blob_content = "\n".join(eligible_reward_codes + [pre_existing_reward.code])

    reward_agent.process_csv(
        retailer_slug=reward_config.retailer_slug,
        blob_name="test-retailer/available-rewards/test-reward/new-reward.csv",
        blob_content=blob_content,
        db_session=db_session,
    )

    rewards = _get_reward_rows(db_session)
    assert len(rewards) == 4
    assert all(v in [reward.code for reward in rewards] for v in eligible_reward_codes)
    # We should be sentry warned about the existing token
    assert capture_message_spy.call_count == 1
    assert (
        "Pre-existing reward codes found in test-retailer/available-rewards/test-reward/new-reward.csv:\nrows: 4"
        == capture_message_spy.call_args_list[0][0][0]
    )


def test_import_agent__process_csv_same_reward_slug_not_soft_deleted(setup: SetupType, mocker: MockerFixture) -> None:
    """
    Test that a reward code imported for the same reward slug, where that existing reward code HAS
    been soft-deleted, will cause an error to be reported and will not be imported
    """
    mocker.patch("app.scheduler.sentry_sdk")
    db_session, reward_config, pre_existing_reward = setup
    pre_existing_reward.deleted = True
    db_session.commit()
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk
    from app.scheduler import sentry_sdk as scheduler_sentry_sdk

    mocker.spy(scheduler_sentry_sdk, "capture_message")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.SENTRY_DSN = "SENTRY_DSN"
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    reward_agent = RewardImportAgent()
    eligible_reward_codes = ["reward1", "reward2", "reward3"]

    blob_content = "\n".join(eligible_reward_codes + [pre_existing_reward.code])

    reward_agent.process_csv(
        retailer_slug=reward_config.retailer_slug,
        blob_name="test-retailer/available-rewards/test-reward/new-reward.csv",
        blob_content=blob_content,
        db_session=db_session,
    )

    rewards = _get_reward_rows(db_session)
    assert len(rewards) == 4
    assert all(v in [reward.code for reward in rewards] for v in eligible_reward_codes)
    # We should be sentry warned about the existing token
    assert capture_message_spy.call_count == 1
    assert (
        "Pre-existing reward codes found in test-retailer/available-rewards/test-reward/new-reward.csv:\nrows: 4"
        == capture_message_spy.call_args_list[0][0][0]
    )


def test_import_agent__process_csv_no_reward_config(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, reward_config, _ = setup

    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    reward_agent = RewardImportAgent()
    with pytest.raises(BlobProcessingError) as exc_info:
        reward_agent.process_csv(
            retailer_slug=reward_config.retailer_slug,
            blob_name="test-retailer/available-rewards/incorrect-reward-type/new-rewards.csv",
            blob_content="reward1\nreward2\nreward3",
            db_session=db_session,
        )
    assert exc_info.value.args == ("No RewardConfig found for reward_slug incorrect-reward-type",)


def test_import_agent__process_csv_no_reward_type_in_path(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, reward_config, _ = setup

    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    reward_agent = RewardImportAgent()
    with pytest.raises(BlobProcessingError) as exc_info:
        reward_agent.process_csv(
            retailer_slug=reward_config.retailer_slug,
            blob_name="test-retailer/available-rewards/new-rewards.csv",
            blob_content="reward1\nreward2\nreward3",
            db_session=db_session,
        )
    assert exc_info.value.args == (
        "No reward_slug path section found (not enough values to unpack (expected 2, got 1))",
    )


def test_updates_agent__process_csv(setup: SetupType, mocker: MockerFixture) -> None:
    # GIVEN
    db_session, reward_config, _ = setup

    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    reward_agent = RewardUpdatesAgent()
    mock__process_updates = mocker.patch.object(reward_agent, "_process_updates")
    blob_name = "/test-retailer/reward-updates/test.csv"
    content = """
TEST12345678,2021-07-30,cancelled
TEST87654321,2021-07-21,redeemed
TEST87654322,2021-07-30,cancelled
TEST87654322,2021-07-30,redeemed
""".strip()

    reward_agent.process_csv(
        retailer_slug=reward_config.retailer_slug,
        blob_name=blob_name,
        blob_content=content,
        db_session=db_session,
    )
    expected_reward_update_rows_by_code = defaultdict(
        list[RewardUpdateRow],
        {
            "TEST12345678": [
                RewardUpdateRow(
                    row_num=1,
                    data=RewardUpdateSchema(
                        code="TEST12345678", date="2021-07-30", status=RewardUpdateStatuses.CANCELLED
                    ),
                )
            ],
            "TEST87654321": [
                RewardUpdateRow(
                    row_num=2,
                    data=RewardUpdateSchema(
                        code="TEST87654321", date="2021-07-21", status=RewardUpdateStatuses.REDEEMED
                    ),
                )
            ],
            "TEST87654322": [
                RewardUpdateRow(
                    row_num=3,
                    data=RewardUpdateSchema(
                        code="TEST87654322", date="2021-07-30", status=RewardUpdateStatuses.CANCELLED
                    ),
                ),
                RewardUpdateRow(
                    row_num=4,
                    data=RewardUpdateSchema(
                        code="TEST87654322", date="2021-07-30", status=RewardUpdateStatuses.REDEEMED
                    ),
                ),
            ],
        },
    )
    mock__process_updates.assert_called_once_with(
        retailer_slug="test-retailer",
        blob_name=blob_name,
        db_session=db_session,
        reward_update_rows_by_code=expected_reward_update_rows_by_code,
    )


def test_updates_agent__process_csv_reward_code_fails_non_validating_rows(
    setup: SetupType, mocker: MockerFixture
) -> None:
    """If non-validating values are encountered, sentry should log a msg"""
    # GIVEN
    db_session, reward_config, reward = setup
    reward.allocated = True
    db_session.commit()

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch.object(RewardUpdatesAgent, "enqueue_reward_updates")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO
    reward_agent = RewardUpdatesAgent()
    blob_name = "/test-retailer/reward-updates/test.csv"
    bad_date = "20210830"
    bad_status = "nosuchstatus"
    content = f"""
TSTCD1234,2021-07-30,redeemed
TEST12345678,{bad_date},redeemed
TEST666666,2021-07-30,{bad_status}
""".strip()

    # WHEN
    reward_agent.process_csv(
        retailer_slug=reward_config.retailer_slug,
        blob_name=blob_name,
        blob_content=content,
        db_session=db_session,
    )

    # THEN
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert f"time data '{bad_date}' does not match format '%Y-%m-%d'" in expected_error_msg
    assert f"'{bad_status}' is not a valid RewardUpdateStatuses" in expected_error_msg


def test_updates_agent__process_csv_reward_code_fails_malformed_csv_rows(
    setup: SetupType, mocker: MockerFixture
) -> None:
    """If a bad CSV row in encountered, sentry should log a msg"""
    # GIVEN
    db_session, reward_config, _ = setup

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch.object(RewardUpdatesAgent, "enqueue_reward_updates")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO
    reward_agent = RewardUpdatesAgent()
    blob_name = "/test-retailer/reward-updates/test.csv"
    content = "TEST87654321,2021-07-30\nTEST12345678,redeemed\n"

    # WHEN
    reward_agent.process_csv(
        retailer_slug=reward_config.retailer_slug,
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
    db_session, reward_config, reward = setup
    reward.allocated = True
    db_session.commit()

    mock_enqueue = mocker.patch.object(RewardUpdatesAgent, "enqueue_reward_updates", autospec=True)

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    reward_agent = RewardUpdatesAgent()
    blob_name = "/test-retailer/reward-updates/test.csv"
    data = RewardUpdateSchema(
        code=reward.code,
        date="2021-07-30",
        status=RewardUpdateStatuses("redeemed"),
    )
    reward_update_rows_by_code: DefaultDict[str, List[RewardUpdateRow]] = defaultdict(list[RewardUpdateRow])
    reward_update_rows_by_code[reward.code].append(RewardUpdateRow(data=data, row_num=1))

    # WHEN
    reward_agent._process_updates(
        retailer_slug=reward_config.retailer_slug,
        reward_update_rows_by_code=reward_update_rows_by_code,
        blob_name=blob_name,
        db_session=db_session,
    )
    reward_update_rows = _get_reward_update_rows(db_session, [reward.code])

    assert len(reward_update_rows) == 1
    assert reward_update_rows[0].reward_uuid == reward.id
    assert reward_update_rows[0].status == RewardUpdateStatuses.REDEEMED
    assert isinstance(reward_update_rows[0].date, datetime.date)
    assert str(reward_update_rows[0].date) == "2021-07-30"
    assert capture_message_spy.call_count == 0  # Should be no errors
    assert mock_enqueue.called


def test_updates_agent__process_updates_reward_code_not_allocated(setup: SetupType, mocker: MockerFixture) -> None:
    """If the reward is not allocated, it should be soft-deleted"""
    # GIVEN
    db_session, reward_config, reward = setup
    reward.allocated = False
    db_session.commit()

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch.object(RewardUpdatesAgent, "enqueue_reward_updates")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO
    reward_agent = RewardUpdatesAgent()
    mocker.patch.object(reward_agent, "_report_unknown_codes", autospec=True)
    blob_name = "/test-retailer/reward-updates/test.csv"
    data = RewardUpdateSchema(
        code=reward.code,
        date="2021-07-30",
        status=RewardUpdateStatuses("redeemed"),
    )
    reward_update_rows_by_code: DefaultDict[str, List[RewardUpdateRow]] = defaultdict(list[RewardUpdateRow])
    reward_update_rows_by_code[reward.code].append(RewardUpdateRow(data=data, row_num=1))

    # WHEN
    reward_agent._process_updates(
        db_session=db_session,
        retailer_slug=reward_config.retailer_slug,
        reward_update_rows_by_code=reward_update_rows_by_code,
        blob_name=blob_name,
    )
    reward_update_rows = _get_reward_update_rows(db_session, [reward.code])

    # THEN
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert "Unallocated reward codes found" in expected_error_msg
    assert len(reward_update_rows) == 0


def test_updates_agent__process_updates_reward_code_does_not_exist(setup: SetupType, mocker: MockerFixture) -> None:
    """The reward does not exist"""
    # GIVEN
    db_session, reward_config, _ = setup

    from app.imports.agents.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    mocker.patch.object(RewardUpdatesAgent, "enqueue_reward_updates")
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO
    reward_agent = RewardUpdatesAgent()
    mocker.patch.object(reward_agent, "_process_unallocated_codes", autospec=True)
    blob_name = "/test-retailer/reward-updates/test.csv"
    bad_reward_code = "IDONOTEXIST"
    data = RewardUpdateSchema(
        code=bad_reward_code,
        date="2021-07-30",
        status=RewardUpdateStatuses("cancelled"),
    )
    reward_update_rows_by_code: DefaultDict[str, List[RewardUpdateRow]] = defaultdict(list[RewardUpdateRow])
    reward_update_rows_by_code[bad_reward_code].append(RewardUpdateRow(data=data, row_num=1))

    # WHEN
    reward_agent._process_updates(
        retailer_slug=reward_config.retailer_slug,
        reward_update_rows_by_code=reward_update_rows_by_code,
        blob_name=blob_name,
        db_session=db_session,
    )
    reward_update_rows = _get_reward_update_rows(db_session, [bad_reward_code])

    # THEN
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert (
        "Unknown reward codes found while processing /test-retailer/reward-updates/test.csv, rows: 1"
        in expected_error_msg
    )
    assert len(reward_update_rows) == 0


class Blob:
    def __init__(self, name: str):
        self.name = name


def test_process_blobs(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, _, _ = setup
    MockBlobServiceClient = mocker.patch("app.imports.agents.file_agent.BlobServiceClient", autospec=True)
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)

    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    reward_agent = RewardUpdatesAgent()
    container_client = mocker.patch.object(reward_agent, "container_client", spec=ContainerClient)
    mock_process_csv = mocker.patch.object(reward_agent, "process_csv")
    mock_move_blob = mocker.patch.object(reward_agent, "move_blob")
    file_name = "test-retailer/reward-updates/update.csv"
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(file_name),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value = mocker.MagicMock(spec=BlobClient)

    reward_agent.process_blobs("test-retailer", db_session=db_session)

    reward_file_log_row = db_session.execute(
        select(RewardFileLog).where(
            RewardFileLog.file_name == file_name, RewardFileLog.file_agent_type == FileAgentType.UPDATE
        )
    ).scalar_one_or_none()

    assert reward_file_log_row
    mock_process_csv.assert_called_once()
    mock_move_blob.assert_called_once()


def test_process_blobs_unicodedecodeerror(capture: LogCapture, setup: SetupType, mocker: MockerFixture) -> None:
    db_session, _, _ = setup
    MockBlobServiceClient = mocker.patch("app.imports.agents.file_agent.BlobServiceClient", autospec=True)
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)

    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    reward_agent = RewardUpdatesAgent()
    container_client = mocker.patch.object(reward_agent, "container_client", spec=ContainerClient)
    mock_process_csv = mocker.patch.object(reward_agent, "process_csv")
    mock_move_blob = mocker.patch.object(reward_agent, "move_blob")
    blob_filename = "test-retailer/reward-updates/update.csv"
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(blob_filename),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value.download_blob.return_value.readall.return_value = (
        b"\xca,2021,09,13,cancelled"
    )

    reward_agent.process_blobs("test-retailer", db_session=db_session)

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

    reward_agent = RewardUpdatesAgent()
    mock_process_csv = mocker.patch.object(reward_agent, "process_csv")
    container_client = mocker.patch.object(reward_agent, "container_client", spec=ContainerClient)
    mock_move_blob = mocker.patch.object(reward_agent, "move_blob")
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob("test-retailer/reward-updates/update.docx"),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value = mocker.MagicMock(spec=BlobClient)
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.BLOB_ERROR_CONTAINER = "ERROR-CONTAINER"

    reward_agent.process_blobs("test-retailer", db_session=db_session)
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    assert (
        "test-retailer/reward-updates/update.docx does not have .csv ext. Moving to ERROR-CONTAINER for checking"
        == capture_message_spy.call_args.args[0]
    )
    mock_move_blob.assert_called_once()
    assert mock_move_blob.call_args[0][0] == "ERROR-CONTAINER"
    mock_process_csv.assert_not_called()


def test_process_blobs_filename_is_duplicate(setup: SetupType, mocker: MockerFixture) -> None:
    db_session, _, _ = setup
    file_name = "test-retailer/reward-updates/update.csv"
    db_session.add(
        RewardFileLog(
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

    reward_agent = RewardUpdatesAgent()
    mock_process_csv = mocker.patch.object(reward_agent, "process_csv")
    container_client = mocker.patch.object(reward_agent, "container_client", spec=ContainerClient)
    mock_move_blob = mocker.patch.object(reward_agent, "move_blob")
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(file_name),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value = mocker.MagicMock(spec=BlobClient)
    mock_settings = mocker.patch("app.imports.agents.file_agent.settings")
    mock_settings.BLOB_ERROR_CONTAINER = "ERROR-CONTAINER"

    reward_agent.process_blobs("test-retailer", db_session=db_session)
    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    assert (
        "test-retailer/reward-updates/update.csv is a duplicate. Moving to ERROR-CONTAINER for checking"
        == capture_message_spy.call_args.args[0]
    )
    mock_move_blob.assert_called_once()
    assert mock_move_blob.call_args[0][0] == "ERROR-CONTAINER"
    mock_process_csv.assert_not_called()


def test_process_blobs_filename_is_not_duplicate(setup: SetupType, mocker: MockerFixture) -> None:
    """A filename exists in the log, but the file agent type is different"""
    db_session, _, _ = setup
    file_name = "test-retailer/reward-updates/update.csv"
    db_session.add(
        RewardFileLog(
            file_name=file_name,
            file_agent_type=FileAgentType.IMPORT,
        )
    )
    db_session.commit()
    MockBlobServiceClient = mocker.patch("app.imports.agents.file_agent.BlobServiceClient", autospec=True)
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)

    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    reward_agent = RewardUpdatesAgent()
    container_client = mocker.patch.object(reward_agent, "container_client", spec=ContainerClient)
    mock_process_csv = mocker.patch.object(reward_agent, "process_csv")
    mock_move_blob = mocker.patch.object(reward_agent, "move_blob")
    file_name = "test-retailer/reward-updates/update.csv"
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(file_name),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value = mocker.MagicMock(spec=BlobClient)

    reward_agent.process_blobs("test-retailer", db_session=db_session)

    reward_file_log_row = db_session.execute(
        select(RewardFileLog).where(
            RewardFileLog.file_name == file_name, RewardFileLog.file_agent_type == FileAgentType.UPDATE
        )
    ).scalar_one_or_none()

    assert reward_file_log_row  # The new record
    mock_process_csv.assert_called_once()
    mock_move_blob.assert_called_once()


def test_move_blob(mocker: MockerFixture) -> None:
    # GIVEN
    MockBlobServiceClient = mocker.patch("app.imports.agents.file_agent.BlobServiceClient", autospec=True)
    MockBlobServiceClient.from_connection_string.return_value = mocker.MagicMock(spec=BlobServiceClient)
    mock_src_blob_client = mocker.patch("app.imports.agents.file_agent.BlobClient", autospec=True)
    mock_src_blob_client.url = "https://a-blob-url"
    src_blob_lease_client = mocker.patch("app.imports.agents.file_agent.BlobLeaseClient", autospec=True)

    reward_agent = RewardUpdatesAgent()
    mock_src_blob_client.blob_name = blob_name = "/test-retailer/reward-updates/test.csv"

    blob_service_client = mocker.patch.object(reward_agent, "blob_service_client")
    mock_dst_blob_client = mocker.MagicMock()
    blob_service_client.delete_blob = mocker.MagicMock()
    blob_service_client.get_blob_client.return_value = mock_dst_blob_client

    # WHEN
    reward_agent.move_blob(
        "DESTINATION-CONTAINER",
        mock_src_blob_client,
        src_blob_lease_client,
    )
    blob = f"{datetime.datetime.utcnow().strftime('%Y/%m/%d/%H%M')}/{blob_name}"

    # THEN
    blob_service_client.get_blob_client.assert_called_once_with("DESTINATION-CONTAINER", blob)
    mock_dst_blob_client.start_copy_from_url.assert_called_once_with("https://a-blob-url")
    mock_src_blob_client.delete_blob.assert_called_once()


def test_enqueue_reward_updates(
    setup: SetupType, mocker: MockerFixture, reward_status_adjustment_task_type: TaskType
) -> None:
    db_session, _, reward = setup

    mock_sync_create_many_tasks = mocker.patch("app.imports.agents.file_agent.sync_create_many_tasks")
    mock_sync_create_many_tasks.return_value = [mock.MagicMock(spec=RetryTask, retry_task_id=1)]
    mock_enqueue_many_retry_tasks = mocker.patch("app.imports.agents.file_agent.enqueue_many_retry_tasks")
    mock_redis = mocker.patch("app.imports.agents.file_agent.redis")

    today = datetime.date.today()
    reward_update = RewardUpdate(
        reward=reward,
        date=today,
        status=RewardUpdateStatuses.REDEEMED,
    )

    RewardUpdatesAgent.enqueue_reward_updates(db_session, [reward_update])
    mock_sync_create_many_tasks.assert_called_once_with(
        db_session,
        params_list=[
            {
                "date": datetime.datetime.combine(today, datetime.time(0, 0)).timestamp(),
                "retailer_slug": "test-retailer",
                "status": "redeemed",
                "reward_uuid": reward.id,
            }
        ],
        task_type_name="reward-status-adjustment",
    )
    mock_enqueue_many_retry_tasks.assert_called_once_with(
        db_session,
        retry_tasks_ids=[1],
        connection=mock_redis,
    )


def test_enqueue_reward_updates_exception(
    setup: SetupType, mocker: MockerFixture, reward_status_adjustment_task_type: TaskType
) -> None:
    db_session, _, reward = setup

    mock_sentry_sdk = mocker.patch("app.imports.agents.file_agent.sentry_sdk")
    mock_enqueue_many_retry_tasks = mocker.patch("app.imports.agents.file_agent.enqueue_many_retry_tasks")
    error = redis.RedisError("Fake connection error")
    mock_enqueue_many_retry_tasks.side_effect = error
    today = datetime.date.today()

    reward_update = RewardUpdate(
        reward=reward,
        date=today,
        status=RewardUpdateStatuses.REDEEMED,
    )

    RewardUpdatesAgent.enqueue_reward_updates(db_session, [reward_update])

    mock_sentry_sdk.capture_exception.assert_called_once_with(error)
    assert db_session.execute(select(func.count()).select_from(RetryTask)).scalar_one() == 0