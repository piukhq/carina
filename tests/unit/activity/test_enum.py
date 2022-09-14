from datetime import datetime, timezone
from uuid import uuid4

import pytest

from pytest_mock import MockFixture

from app.activity_utils.enums import ActivityType, _try_parse_account_url_path


def test_get_reward_status_activity_data_no_pending(mocker: MockFixture) -> None:
    fake_now = datetime.now(tz=timezone.utc)

    mock_datetime = mocker.patch("app.activity_utils.enums.datetime")
    mock_datetime.now.return_value = fake_now
    mock_datetime.fromtimestamp = datetime.fromtimestamp

    user_uuid = str(uuid4())
    reward_slug = "test-reward"
    retailer_slug = "test-retailer"
    reward_uuid = str(uuid4())
    activity_datetime = datetime.now(tz=timezone.utc)
    account_url_path = f"/loyalty/{retailer_slug}/accounts/{user_uuid}/rewards"

    payload = ActivityType.get_reward_status_activity_data(
        account_url_path=account_url_path,
        retailer_slug=retailer_slug,
        reward_slug=reward_slug,
        activity_timestamp=activity_datetime.timestamp(),
        reward_uuid=reward_uuid,
        pending_reward_id=None,
    )

    assert payload == {
        "type": ActivityType.REWARD_STATUS.name,
        "datetime": fake_now,
        "underlying_datetime": activity_datetime,
        "summary": f'{retailer_slug} Reward "issued"',
        "reasons": ["Reward goal met"],
        "activity_identifier": reward_uuid,
        "user_id": user_uuid,
        "associated_value": "issued",
        "retailer": retailer_slug,
        "campaigns": [],
        "data": {
            "new_status": "issued",
            "reward_slug": reward_slug,
        },
    }


def test_get_reward_status_activity_data_pending(mocker: MockFixture) -> None:
    fake_now = datetime.now(tz=timezone.utc)

    mock_datetime = mocker.patch("app.activity_utils.enums.datetime")
    mock_datetime.now.return_value = fake_now
    mock_datetime.fromtimestamp = datetime.fromtimestamp

    pending_reward_id = str(uuid4())
    user_uuid = str(uuid4())
    reward_slug = "test-reward"
    retailer_slug = "test-retailer"
    reward_uuid = str(uuid4())
    activity_datetime = datetime.now(tz=timezone.utc)
    account_url_path = f"/loyalty/{retailer_slug}/accounts/{user_uuid}/rewards"

    payload = ActivityType.get_reward_status_activity_data(
        account_url_path=account_url_path,
        retailer_slug=retailer_slug,
        reward_slug=reward_slug,
        activity_timestamp=activity_datetime.timestamp(),
        reward_uuid=reward_uuid,
        pending_reward_id=pending_reward_id,
    )

    assert payload == {
        "type": ActivityType.REWARD_STATUS.name,
        "datetime": fake_now,
        "underlying_datetime": activity_datetime,
        "summary": f'{retailer_slug} Reward "issued"',
        "reasons": ["Pending Reward converted"],
        "activity_identifier": reward_uuid,
        "user_id": user_uuid,
        "associated_value": "issued",
        "retailer": retailer_slug,
        "campaigns": [],
        "data": {
            "new_status": "issued",
            "original_status": "pending",
            "pending_reward_id": pending_reward_id,
            "reward_slug": reward_slug,
        },
    }


@pytest.mark.parametrize(
    argnames=("account_url_path", "expected_result", "logger_called"),
    argvalues=(
        pytest.param(
            "/loyalty/test-retailer/accounts/89f60a4f-c9ed-4cfe-9e76-177077c71552/rewards",
            "89f60a4f-c9ed-4cfe-9e76-177077c71552",
            False,
            id="test_correct_account_url_path",
        ),
        pytest.param(
            "/accounts/89f60a4f-c9ed-4cfe-9e76-177077c71552/rewards",
            "89f60a4f-c9ed-4cfe-9e76-177077c71552",
            False,
            id="test_correct_enough_account_url_path",
        ),
        pytest.param(
            "/other/url/path",
            "/other/url/path",
            True,
            id="test_account_url_path_formatted_unexpectedly",
        ),
        pytest.param(
            "/accounts/not-a-uuid/rewards",
            "/accounts/not-a-uuid/rewards",
            True,
            id="test_extracted_value_not_a_valid_uuid",
        ),
    ),
)
def test__try_parse_account_url_path(
    mocker: MockFixture, account_url_path: str, expected_result: str, logger_called: bool
) -> None:
    mock_logger = mocker.patch("app.activity_utils.enums.logger")

    user_id = _try_parse_account_url_path(account_url_path)
    assert user_id == expected_result

    if logger_called:
        mock_logger.warning.assert_called_with(
            "failed to extract account_holder_uuid from path '%s', using whole path as user_id", account_url_path
        )

    else:
        mock_logger.warning.assert_not_called()
