import uuid

from copy import deepcopy
from typing import TYPE_CHECKING
from uuid import uuid4

from fastapi import status
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKeyValue
from sqlalchemy import func
from sqlalchemy.future import select

from asgi import app
from carina.core.config import settings
from carina.enums import RewardCampaignStatuses, RewardTypeStatuses
from carina.models import Retailer, RewardCampaign
from carina.models.retailer import FetchType
from carina.models.reward import Allocation, Reward, RewardConfig
from tests.conftest import SetupType
from tests.fixtures import HttpErrors

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


client = TestClient(app)
auth_headers = {"Authorization": f"token {settings.CARINA_API_AUTH_TOKEN}"}
payload = {
    "account_url": "http://test.url/",
    "count": 1,
    "campaign_slug": "test-campaign",
}


def _get_retry_task_and_values(
    db_session: "Session", task_type_id: int, reward_config_id: int
) -> tuple[RetryTask, list[str]]:
    values: list[str] = []
    retry_task: RetryTask = (
        db_session.execute(
            select(RetryTask).where(
                RetryTask.task_type_id == task_type_id,
                RetryTask.retry_task_id == TaskTypeKeyValue.retry_task_id,
                TaskTypeKeyValue.value == str(reward_config_id),
            )
        )
        .scalars()
        .first()
    )
    if retry_task:
        values = [value.value for value in retry_task.task_type_key_values]

    return retry_task, values


def _get_retry_tasks_ids_by_task_type_id(
    db_session: "Session", task_type_id: int, reward_config_id: int
) -> list[RetryTask]:
    retry_tasks = (
        db_session.execute(
            select(RetryTask).where(
                RetryTask.task_type_id == task_type_id,
                TaskTypeKeyValue.value == str(reward_config_id),
            )
        )
        .unique()
        .scalars()
        .all()
    )
    return [task.retry_task_id for task in retry_tasks]


def test_post_reward_allocation_happy_path(
    setup: SetupType, mocker: MockerFixture, reward_issuance_task_type: TaskType
) -> None:
    db_session, reward_config, reward = setup
    mock_enqueue_tasks = mocker.patch("carina.api.endpoints.reward.enqueue_many_tasks")

    assert reward.allocated is False

    resp = client.post(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/allocation",
        json=payload,
        headers={**auth_headers, "idempotency-token": str(uuid4())},
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}

    retry_task, task_params_values = _get_retry_task_and_values(
        db_session, reward_issuance_task_type.task_type_id, reward_config.id
    )
    task_params = retry_task.get_params()

    db_session.refresh(reward)

    assert retry_task is not None
    assert "pending_reward_id" not in task_params
    assert payload["account_url"] in task_params_values
    assert payload["campaign_slug"] in task_params_values
    assert str(reward_config.id) in task_params_values
    assert str(reward.id) not in task_params_values
    assert reward.allocated is False
    mock_enqueue_tasks.assert_called_once_with(retry_tasks_ids=[retry_task.retry_task_id])


def test_post_reward_allocation_with_count(
    setup: SetupType, mocker: MockerFixture, reward_issuance_task_type: TaskType
) -> None:
    db_session, reward_config, _ = setup
    mock_enqueue_tasks = mocker.patch("carina.api.endpoints.reward.enqueue_many_tasks")
    reward_allocation_count = 3

    payload_with_count = deepcopy(payload)
    payload_with_count["count"] = str(reward_allocation_count)

    resp = client.post(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/allocation",
        json=payload_with_count,
        headers={**auth_headers, "idempotency-token": str(uuid4())},
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}
    retry_task_ids = _get_retry_tasks_ids_by_task_type_id(
        db_session, reward_issuance_task_type.task_type_id, reward_config.id
    )

    assert len(retry_task_ids) == reward_allocation_count
    mock_enqueue_tasks.assert_called_once_with(retry_tasks_ids=retry_task_ids)


def test_post_reward_allocation_with_pending_reward_id(
    setup: SetupType, mocker: MockerFixture, reward_issuance_task_type: TaskType
) -> None:
    db_session, reward_config, _ = setup
    mock_enqueue_tasks = mocker.patch("carina.api.endpoints.reward.enqueue_many_tasks")

    payload_with_pending_reward_id = deepcopy(payload)
    payload_with_pending_reward_id["pending_reward_id"] = str(uuid.uuid4())
    idempotency_token = str(uuid.uuid4())

    resp = client.post(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/allocation",
        json=payload_with_pending_reward_id,
        headers={"idempotency-token": idempotency_token, **auth_headers},
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}

    retry_task, task_params_values = _get_retry_task_and_values(
        db_session, reward_issuance_task_type.task_type_id, reward_config.id
    )

    assert retry_task is not None
    assert payload_with_pending_reward_id["pending_reward_id"] in task_params_values
    retry_task_ids = _get_retry_tasks_ids_by_task_type_id(
        db_session, reward_issuance_task_type.task_type_id, reward_config.id
    )
    mock_enqueue_tasks.assert_called_once_with(retry_tasks_ids=retry_task_ids)


def test_post_reward_allocation_wrong_retailer(setup: SetupType, reward_issuance_task_type: TaskType) -> None:
    db_session, reward_config, _ = setup

    resp = client.post(
        f"{settings.API_PREFIX}/WRONG-RETAILER/rewards/{reward_config.reward_slug}/allocation",
        json=payload,
        headers={**auth_headers, "idempotency-token": str(uuid4())},
    )

    assert resp.status_code == HttpErrors.INVALID_RETAILER.value.status_code
    assert resp.json() == HttpErrors.INVALID_RETAILER.value.detail

    retry_task, _ = _get_retry_task_and_values(db_session, reward_issuance_task_type.task_type_id, reward_config.id)
    assert retry_task is None


def test_post_reward_allocation_wrong_reward_type(setup: SetupType, reward_issuance_task_type: TaskType) -> None:
    db_session, reward_config, _ = setup

    resp = client.post(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/WRONG-TYPE/allocation",
        json=payload,
        headers={**auth_headers, "idempotency-token": str(uuid4())},
    )

    assert resp.status_code == HttpErrors.UNKNOWN_REWARD_TYPE.value.status_code
    assert resp.json() == HttpErrors.UNKNOWN_REWARD_TYPE.value.detail

    retry_task, _ = _get_retry_task_and_values(db_session, reward_issuance_task_type.task_type_id, reward_config.id)
    assert retry_task is None


def test_post_reward_allocation_no_more_rewards(
    setup: SetupType, mocker: MockerFixture, reward_issuance_task_type: TaskType
) -> None:
    db_session, reward_config, reward = setup
    reward.allocated = True
    db_session.commit()

    mock_enqueue_tasks = mocker.patch("carina.api.endpoints.reward.enqueue_many_tasks")
    idempotency_token = str(uuid.uuid4())

    resp = client.post(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/allocation",
        json=payload,
        headers={"idempotency-token": idempotency_token, **auth_headers},
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}

    retry_task, task_params_values = _get_retry_task_and_values(
        db_session, reward_issuance_task_type.task_type_id, reward_config.id
    )

    assert retry_task is not None
    assert payload["account_url"] in task_params_values
    assert str(reward_config.id) in task_params_values
    mock_enqueue_tasks.assert_called_once_with(retry_tasks_ids=[retry_task.retry_task_id])


def test_post_reward_allocation_existing_idempotency_token(
    setup: SetupType, mocker: MockerFixture, reward_issuance_task_type: TaskType
) -> None:
    db_session, reward_config, _ = setup
    mock_enqueue_tasks = mocker.patch("carina.api.endpoints.reward.enqueue_many_tasks")
    mock_sentry = mocker.patch("carina.crud.reward.sentry_sdk")

    idempotency_token = uuid4()

    # First request
    first_allocation_response = client.post(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/allocation",
        json=payload,
        headers={**auth_headers, "idempotency-token": str(idempotency_token)},
    )

    assert first_allocation_response.status_code == status.HTTP_202_ACCEPTED
    assert first_allocation_response.json() == {}

    existing_allocation_request_id = (
        db_session.execute(select(Allocation.id).where(Allocation.idempotency_token == str(idempotency_token)))
    ).scalar_one()

    # Second request with same idempotency_token
    second_allocation_response = client.post(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/allocation",
        json=payload,
        headers={**auth_headers, "idempotency-token": str(idempotency_token)},
    )

    assert second_allocation_response.status_code == status.HTTP_202_ACCEPTED
    assert second_allocation_response.json() == {}

    # Only one reward_issuance task created
    retry_task_ids = _get_retry_tasks_ids_by_task_type_id(
        db_session, reward_issuance_task_type.task_type_id, reward_config.id
    )
    assert len(retry_task_ids) == 1
    mock_enqueue_tasks.assert_called_once_with(retry_tasks_ids=retry_task_ids)

    # Allocation table only consists one entry, from the first request
    assert db_session.execute(select(func.count()).select_from(Allocation)).scalar() == 1

    error_msg = (
        f"IntegrityError on reward allocation when creating reward issuance tasks "
        f"account url: {payload['account_url']} (retailer slug: {reward_config.retailer.slug}).\n"
        f"New allocation request for (reward slug: {reward_config.reward_slug}) is using a conflicting token "
        f"{idempotency_token} with existing allocation request of id: {existing_allocation_request_id}"
    )
    assert error_msg in mock_sentry.capture_message.call_args.args[0]


def test_post_reward_allocation_invalid_idempotency_token(
    setup: SetupType,
) -> None:
    reward_config = setup.reward_config

    allocation_response = client.post(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/allocation",
        json=payload,
        headers={**auth_headers, "idempotency-token": "invalid-token"},
    )

    assert allocation_response.status_code == HttpErrors.MISSING_OR_INVALID_IDEMPOTENCY_TOKEN_HEADER.value.status_code
    assert allocation_response.json() == HttpErrors.MISSING_OR_INVALID_IDEMPOTENCY_TOKEN_HEADER.value.detail


def test_post_reward_allocation_missing_idempotency_token(
    setup: SetupType,
) -> None:
    reward_config = setup.reward_config

    allocation_response = client.post(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/allocation",
        json=payload,
        headers=auth_headers,
    )

    assert allocation_response.status_code == HttpErrors.MISSING_OR_INVALID_IDEMPOTENCY_TOKEN_HEADER.value.status_code
    assert allocation_response.json() == HttpErrors.MISSING_OR_INVALID_IDEMPOTENCY_TOKEN_HEADER.value.detail


def test_insert_reward_campaign_happy_path(
    setup: SetupType,
    retailer: Retailer,
) -> None:
    db_session, _, reward = setup
    mock_campaign_slug = "test-campaign"

    reward_campaign_payload = {
        "status": RewardTypeStatuses.ACTIVE,
        "campaign_slug": mock_campaign_slug,
    }
    resp = client.put(
        f"{settings.API_PREFIX}/{reward.reward_config.retailer.slug}/{reward.reward_config.reward_slug}/campaign",
        json=reward_campaign_payload,
        headers=auth_headers,
    )

    assert resp.status_code == status.HTTP_201_CREATED
    assert resp.json() == {}

    existing_reward_campaign: RewardCampaign = (
        db_session.execute(
            select(RewardCampaign).where(
                RewardCampaign.campaign_slug == mock_campaign_slug, RewardCampaign.retailer_id == retailer.id
            )
        )
    ).scalar_one()
    assert existing_reward_campaign.campaign_status == RewardCampaignStatuses.ACTIVE


def test_update_reward_campaign_happy_path(
    setup: SetupType,
    retailer: Retailer,
    reward_campaign: RewardCampaign,
) -> None:
    db_session, _, reward = setup

    reward_campaign_payload = {
        "status": RewardTypeStatuses.ENDED,
        "campaign_slug": reward_campaign.campaign_slug,
    }
    resp = client.put(
        f"{settings.API_PREFIX}/{reward.reward_config.retailer.slug}/{reward.reward_config.reward_slug}/campaign",
        json=reward_campaign_payload,
        headers=auth_headers,
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == {}

    db_session.refresh(reward_campaign)

    assert reward_campaign.campaign_status == RewardCampaignStatuses.ENDED


def test_reward_campaign_reward_slug_not_found(setup: SetupType) -> None:
    _, reward_config, _ = setup

    reward_campaign_payload = {
        "status": RewardTypeStatuses.ACTIVE,
        "campaign_slug": "test-campaign",
    }
    resp = client.put(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/invalid-reward-slug/campaign",
        json=reward_campaign_payload,
        headers=auth_headers,
    )
    assert resp.status_code == HttpErrors.UNKNOWN_REWARD_TYPE.value.status_code
    assert resp.json() == HttpErrors.UNKNOWN_REWARD_TYPE.value.detail


def test_reward_campaign_invalid_retailer(setup: SetupType) -> None:
    _, reward_config, _ = setup

    reward_campaign_payload = {
        "status": RewardTypeStatuses.ACTIVE,
        "campaign_slug": "test-campaign",
    }
    resp = client.put(
        f"{settings.API_PREFIX}/invalid-retailer-slug/{reward_config.reward_slug}/campaign",
        json=reward_campaign_payload,
        headers=auth_headers,
    )
    assert resp.status_code == HttpErrors.INVALID_RETAILER.value.status_code
    assert resp.json() == HttpErrors.INVALID_RETAILER.value.detail


def test_reward_campaign_validation_error(setup: SetupType) -> None:
    _, reward_config, _ = setup

    reward_campaign_payload = {
        "status": "invalid-status",
        "campaign_slug": "test-campaign",
    }
    resp = client.put(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/{reward_config.reward_slug}/campaign",
        json=reward_campaign_payload,
        headers=auth_headers,
    )

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.json() == {
        "display_message": "Submitted fields are missing or invalid.",
        "code": "FIELD_VALIDATION_ERROR",
        "fields": [
            "status",
        ],
    }


def test_reward_campaign_reward_slug_is_not_active(
    setup: SetupType,
    retailer: Retailer,
    reward_campaign: RewardCampaign,
) -> None:
    db_session, reward_config, reward = setup

    reward_config.status = RewardTypeStatuses.ENDED
    db_session.commit()

    reward_campaign_payload = {
        "status": RewardTypeStatuses.ACTIVE,
        "campaign_slug": "test-campaign",
    }
    resp = client.put(
        f"{settings.API_PREFIX}/{reward.reward_config.retailer.slug}/{reward.reward_config.reward_slug}/campaign",
        json=reward_campaign_payload,
        headers=auth_headers,
    )
    db_session.refresh(reward_config)
    assert reward_config.status == RewardTypeStatuses.ENDED
    assert resp.status_code == HttpErrors.UNKNOWN_REWARD_TYPE.value.status_code
    assert resp.json() == HttpErrors.UNKNOWN_REWARD_TYPE.value.detail


def test_deactivate_reward_type_with_unallocated_rewards(
    setup: SetupType,
    reward_campaign: RewardCampaign,
    pre_loaded_fetch_type: FetchType,
) -> None:
    db_session, _, _ = setup

    reward_campaign.campaign_status = RewardCampaignStatuses.ENDED
    db_session.commit()

    retailer1 = Retailer(slug="retailer1")
    db_session.add(retailer1)
    db_session.flush()
    retailer1_reward_config = RewardConfig(
        reward_slug="retailer1-reward-slug",
        required_fields_values="validity_days: 15",
        retailer_id=retailer1.id,
        fetch_type_id=pre_loaded_fetch_type.id,
        status=RewardTypeStatuses.ACTIVE,
    )
    db_session.add(retailer1_reward_config)
    retailer2 = Retailer(slug="retailer2")
    db_session.add(retailer2)
    db_session.flush()
    retailer2_reward_config = RewardConfig(
        reward_slug="retailer2-reward-slug",
        required_fields_values="validity_days: 15",
        retailer_id=retailer2.id,
        fetch_type_id=pre_loaded_fetch_type.id,
        status=RewardTypeStatuses.ACTIVE,
    )
    db_session.add(retailer2_reward_config)
    db_session.commit()

    db_session.refresh(retailer1_reward_config)
    db_session.refresh(retailer2_reward_config)

    for reward_config in (retailer1_reward_config, retailer2_reward_config):
        db_session.add_all(
            [
                Reward(
                    code=f"{reward_config.retailer.slug}-code1",
                    allocated=False,
                    deleted=False,
                    retailer_id=reward_config.retailer_id,
                    reward_config_id=reward_config.id,
                ),
                Reward(
                    code=f"{reward_config.retailer.slug}-code2",
                    allocated=False,
                    deleted=True,
                    retailer_id=reward_config.retailer_id,
                    reward_config_id=reward_config.id,
                ),
                Reward(
                    code=f"{reward_config.retailer.slug}-code3",
                    allocated=True,
                    deleted=False,
                    retailer_id=reward_config.retailer_id,
                    reward_config_id=reward_config.id,
                ),
                Reward(
                    code=f"{reward_config.retailer.slug}-code4",
                    allocated=True,
                    deleted=True,
                    retailer_id=reward_config.retailer_id,
                    reward_config_id=reward_config.id,
                ),
            ]
        )
    db_session.commit()

    resp = client.delete(
        f"{settings.API_PREFIX}/{retailer1_reward_config.retailer.slug}/rewards/{retailer1_reward_config.reward_slug}",
        headers=auth_headers,
    )

    db_session.refresh(retailer1_reward_config)
    db_session.refresh(retailer2_reward_config)

    assert retailer1_reward_config.status == RewardTypeStatuses.DELETED
    assert retailer2_reward_config.status == RewardTypeStatuses.ACTIVE
    assert resp.status_code == status.HTTP_204_NO_CONTENT
    assert (
        db_session.scalar(
            select(func.count("*")).select_from(Reward).where(Reward.reward_config_id == retailer1_reward_config.id)
        )
        == 2
    )
    assert (
        db_session.scalar(
            select(func.count("*")).select_from(Reward).where(Reward.reward_config_id == retailer2_reward_config.id)
        )
        == 4
    )


def test_deactivate_reward_type_with_api_based_rewards(
    setup: SetupType,
    retailer: Retailer,
    reward_campaign: RewardCampaign,
    jigsaw_fetch_type: FetchType,
) -> None:
    db_session, reward_config, reward = setup

    reward_config.fetch_type = jigsaw_fetch_type
    reward_campaign.campaign_status = RewardCampaignStatuses.ENDED
    reward.allocated = False
    db_session.commit()

    resp = client.delete(
        f"{settings.API_PREFIX}/{retailer.slug}/rewards/{reward_config.reward_slug}",
        headers=auth_headers,
    )
    db_session.refresh(reward_config)
    assert reward_config.status == RewardTypeStatuses.DELETED
    assert resp.status_code == status.HTTP_204_NO_CONTENT


def test_deactivate_reward_type_with_request_error(
    setup: SetupType,
    reward_campaign: RewardCampaign,
) -> None:
    db_session, reward_config, reward = setup

    reward_campaign.campaign_status = RewardCampaignStatuses.ENDED
    reward.allocated = False
    db_session.commit()
    bad_retailer = "potato"
    resp = client.delete(
        f"{settings.API_PREFIX}/{bad_retailer}/rewards/{reward_config.reward_slug}",
        headers=auth_headers,
    )
    db_session.refresh(reward_config)
    assert reward_config.status == RewardTypeStatuses.ACTIVE
    assert resp.status_code == status.HTTP_403_FORBIDDEN
    assert (
        db_session.execute(
            select(func.count("*")).select_from(Reward).where(Reward.reward_config_id == reward_config.id)
        ).scalar_one()
        == 1
    )


def test_deactivate_reward_type_with_active_campaign(
    setup: SetupType,
    retailer: Retailer,
    reward_campaign: RewardCampaign,
) -> None:
    db_session, reward_config, reward = setup

    reward_campaign.campaign_status = RewardCampaignStatuses.ACTIVE
    reward.allocated = False
    db_session.commit()

    resp = client.delete(
        f"{settings.API_PREFIX}/{retailer.slug}/rewards/{reward_config.reward_slug}",
        headers=auth_headers,
    )
    db_session.refresh(reward_config)
    assert reward_config.status == RewardTypeStatuses.ACTIVE
    assert resp.status_code == status.HTTP_409_CONFLICT
    assert (
        db_session.execute(
            select(func.count("*")).select_from(Reward).where(Reward.reward_config_id == reward_config.id)
        ).scalar_one()
        == 1
    )
