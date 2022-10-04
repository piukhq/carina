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
from carina.enums import RewardTypeStatuses
from carina.models.reward import Allocation
from tests.conftest import SetupType
from tests.fixtures import HttpErrors

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


client = TestClient(app)
auth_headers = {"Authorization": f"token {settings.CARINA_API_AUTH_TOKEN}"}
payload = {"account_url": "http://test.url/"}


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


def test_reward_type_cancelled_status_ok(
    setup: SetupType,
    mocker: MockerFixture,
    reward_deletion_task_type: TaskType,
    reward_cancellation_task_type: TaskType,
) -> None:
    db_session, reward_config, _ = setup
    mock_enqueue_tasks = mocker.patch("carina.api.endpoints.reward.enqueue_many_tasks")

    reward_config.status = RewardTypeStatuses.ACTIVE
    db_session.commit()

    resp = client.patch(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/status",
        json={"status": RewardTypeStatuses.CANCELLED},
        headers=auth_headers,
    )
    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}
    db_session.refresh(reward_config)
    assert reward_config.status == RewardTypeStatuses.CANCELLED

    reward_deletion_tasks_ids = _get_retry_tasks_ids_by_task_type_id(
        db_session, reward_deletion_task_type.task_type_id, reward_config.id
    )
    assert len(reward_deletion_tasks_ids) == 1

    reward_cancellation_tasks_ids = _get_retry_tasks_ids_by_task_type_id(
        db_session, reward_cancellation_task_type.task_type_id, reward_config.id
    )
    assert len(reward_cancellation_tasks_ids) == 1

    mock_enqueue_tasks.assert_called_once_with(
        retry_tasks_ids=[*reward_deletion_tasks_ids, *reward_cancellation_tasks_ids]
    )


def test_reward_type_ended_status_ok(
    setup: SetupType,
    mocker: MockerFixture,
    reward_deletion_task_type: TaskType,
    reward_cancellation_task_type: TaskType,
) -> None:
    db_session, reward_config, _ = setup
    mock_enqueue_tasks = mocker.patch("carina.api.endpoints.reward.enqueue_many_tasks")

    reward_config.status = RewardTypeStatuses.ACTIVE
    db_session.commit()

    resp = client.patch(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/status",
        json={"status": RewardTypeStatuses.ENDED},
        headers=auth_headers,
    )
    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}
    db_session.refresh(reward_config)
    assert reward_config.status == RewardTypeStatuses.ENDED

    reward_deletion_tasks_ids = _get_retry_tasks_ids_by_task_type_id(
        db_session, reward_deletion_task_type.task_type_id, reward_config.id
    )
    assert len(reward_deletion_tasks_ids) == 0

    reward_cancellation_tasks_ids = _get_retry_tasks_ids_by_task_type_id(
        db_session, reward_cancellation_task_type.task_type_id, reward_config.id
    )
    assert len(reward_cancellation_tasks_ids) == 0

    mock_enqueue_tasks.assert_not_called()


def test_reward_type_status_bad_status(setup: SetupType) -> None:
    db_session, reward_config, _ = setup

    resp = client.patch(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/status",
        json={"status": "active"},
        headers=auth_headers,
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    db_session.refresh(reward_config)
    assert reward_config.status == RewardTypeStatuses.ACTIVE


def test_reward_type_status_invalid_retailer(setup: SetupType) -> None:
    db_session, reward_config, _ = setup

    resp = client.patch(
        f"{settings.API_PREFIX}/unknown-retailer/rewards/{reward_config.reward_slug}/status",
        json={"status": "cancelled"},
        headers=auth_headers,
    )
    assert resp.status_code == HttpErrors.INVALID_RETAILER.value.status_code
    assert resp.json() == HttpErrors.INVALID_RETAILER.value.detail
    db_session.refresh(reward_config)
    assert reward_config.status == RewardTypeStatuses.ACTIVE


def test_reward_type_status_reward_type_not_found(setup: SetupType) -> None:
    db_session, reward_config, _ = setup

    resp = client.patch(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/invalid-reward-type/status",
        json={"status": "cancelled"},
        headers=auth_headers,
    )
    assert resp.status_code == HttpErrors.UNKNOWN_REWARD_TYPE.value.status_code
    assert resp.json() == HttpErrors.UNKNOWN_REWARD_TYPE.value.detail
    db_session.refresh(reward_config)
    assert reward_config.status == RewardTypeStatuses.ACTIVE


def test_reward_type_status_wrong_reward_config_status(setup: SetupType) -> None:
    db_session, reward_config, _ = setup
    reward_config.status = RewardTypeStatuses.CANCELLED
    db_session.commit()

    resp = client.patch(
        f"{settings.API_PREFIX}/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/status",
        json={"status": "ended"},
        headers=auth_headers,
    )
    assert resp.status_code == HttpErrors.STATUS_UPDATE_FAILED.value.status_code
    assert resp.json() == HttpErrors.STATUS_UPDATE_FAILED.value.detail
    db_session.refresh(reward_config)
    assert reward_config.status == RewardTypeStatuses.CANCELLED
