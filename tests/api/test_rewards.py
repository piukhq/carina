from typing import TYPE_CHECKING, List, Tuple

from fastapi import status
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKeyValue
from sqlalchemy import func
from sqlalchemy.future import select

from app.core.config import settings
from app.enums import RewardTypeStatuses
from asgi import app
from tests.conftest import SetupType
from tests.fixtures import HttpErrors

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


client = TestClient(app)
auth_headers = {"Authorization": f"token {settings.CARINA_API_AUTH_TOKEN}"}
payload = {"account_url": "http://test.url/"}


def _get_retry_task_and_values(
    db_session: "Session", task_type_id: int, reward_config_id: int
) -> Tuple[RetryTask, List[str]]:
    values: List[str] = []
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


def test_post_reward_allocation_happy_path(
    setup: SetupType, mocker: MockerFixture, reward_issuance_task_type: TaskType
) -> None:
    db_session, reward_config, reward = setup
    mocker.patch("app.api.tasks.enqueue_retry_task")

    assert reward.allocated is False

    resp = client.post(
        f"/bpl/vouchers/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/allocation",
        json=payload,
        headers=auth_headers,
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}

    retry_task, task_params_values = _get_retry_task_and_values(
        db_session, reward_issuance_task_type.task_type_id, reward_config.id
    )

    db_session.refresh(reward)

    assert retry_task is not None
    assert payload["account_url"] in task_params_values
    assert str(reward_config.id) in task_params_values
    assert str(reward.id) not in task_params_values
    assert reward.allocated is False


def test_post_reward_allocation_wrong_retailer(setup: SetupType, reward_issuance_task_type: TaskType) -> None:
    db_session, reward_config, _ = setup

    resp = client.post(
        f"/bpl/vouchers/WRONG-RETAILER/rewards/{reward_config.reward_slug}/allocation",
        json=payload,
        headers=auth_headers,
    )

    assert resp.status_code == HttpErrors.INVALID_RETAILER.value.status_code
    assert resp.json() == HttpErrors.INVALID_RETAILER.value.detail

    retry_task, _ = _get_retry_task_and_values(db_session, reward_issuance_task_type.task_type_id, reward_config.id)
    assert retry_task is None


def test_post_reward_allocation_wrong_reward_type(setup: SetupType, reward_issuance_task_type: TaskType) -> None:
    db_session, reward_config, _ = setup

    resp = client.post(
        f"/bpl/vouchers/{reward_config.retailer.slug}/rewards/WRONG-TYPE/allocation",
        json=payload,
        headers=auth_headers,
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

    mocker.patch("app.api.tasks.enqueue_retry_task")

    resp = client.post(
        f"/bpl/vouchers/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/allocation",
        json=payload,
        headers=auth_headers,
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}

    retry_task, task_params_values = _get_retry_task_and_values(
        db_session, reward_issuance_task_type.task_type_id, reward_config.id
    )

    assert retry_task is not None
    assert payload["account_url"] in task_params_values
    assert str(reward_config.id) in task_params_values


def test_reward_type_status_ok(
    setup: SetupType,
    mocker: MockerFixture,
    reward_deletion_task_type: TaskType,
    reward_cancellation_task_type: TaskType,
) -> None:
    db_session, reward_config, _ = setup
    mocker.patch("app.api.tasks.enqueue_many_retry_tasks")

    for transition_status in ("cancelled", "ended"):
        reward_config.status = RewardTypeStatuses.ACTIVE
        db_session.commit()

        resp = client.patch(
            f"/bpl/vouchers/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/status",
            json={"status": transition_status},
            headers=auth_headers,
        )
        assert resp.status_code == status.HTTP_202_ACCEPTED
        assert resp.json() == {}
        db_session.refresh(reward_config)
        assert reward_config.status == RewardTypeStatuses(transition_status)

    assert (
        db_session.scalar(
            select(func.count(RetryTask.retry_task_id)).where(
                RetryTask.task_type_id == reward_deletion_task_type.task_type_id
            )
        )
        == 2
    )
    assert (
        db_session.scalar(
            select(func.count(RetryTask.retry_task_id)).where(
                RetryTask.task_type_id == reward_cancellation_task_type.task_type_id
            )
        )
        == 1
    )


def test_reward_type_status_bad_status(setup: SetupType) -> None:
    db_session, reward_config, _ = setup

    resp = client.patch(
        f"/bpl/vouchers/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/status",
        json={"status": "active"},
        headers=auth_headers,
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    db_session.refresh(reward_config)
    assert reward_config.status == RewardTypeStatuses.ACTIVE


def test_reward_type_status_invalid_retailer(setup: SetupType) -> None:
    db_session, reward_config, _ = setup

    resp = client.patch(
        f"/bpl/vouchers/unknown-retailer/rewards/{reward_config.reward_slug}/status",
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
        f"/bpl/vouchers/{reward_config.retailer.slug}/rewards/invalid-reward-type/status",
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
        f"/bpl/vouchers/{reward_config.retailer.slug}/rewards/{reward_config.reward_slug}/status",
        json={"status": "ended"},
        headers=auth_headers,
    )
    assert resp.status_code == HttpErrors.STATUS_UPDATE_FAILED.value.status_code
    assert resp.json() == HttpErrors.STATUS_UPDATE_FAILED.value.detail
    db_session.refresh(reward_config)
    assert reward_config.status == RewardTypeStatuses.CANCELLED
