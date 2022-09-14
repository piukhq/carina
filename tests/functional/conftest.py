from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable
from uuid import uuid4

import pytest

from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKeyValue
from retry_tasks_lib.utils.synchronous import sync_create_task

from app.core.config import settings
from app.enums import RewardTypeStatuses, RewardUpdateStatuses
from app.models import RetailerFetchType, Reward, RewardUpdate
from app.models.reward import RewardConfig

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@pytest.fixture(scope="function")
def reward_issuance_task_params(reward: Reward) -> dict:
    return {
        "account_url": "http://test.url/",
        "reward_uuid": str(reward.id),
        "code": reward.code,
        "reward_config_id": str(reward.reward_config_id),
        "reward_slug": reward.reward_config.reward_slug,
        "idempotency_token": str(uuid4()),
        "retailer_slug": "test-retailer",
    }


@pytest.fixture(scope="function")
def reward_issuance_task_params_no_reward(reward_config: RewardConfig) -> dict:
    return {
        "account_url": "http://test.url/",
        "reward_config_id": str(reward_config.id),
        "reward_slug": reward_config.reward_slug,
        "idempotency_token": str(uuid4()),
        "retailer_slug": "test-retailer",
    }


@pytest.fixture(scope="function")
def issuance_retry_task(
    db_session: "Session", reward_issuance_task_params: dict, reward_issuance_task_type: TaskType
) -> RetryTask:
    task = RetryTask(task_type_id=reward_issuance_task_type.task_type_id)
    db_session.add(task)
    db_session.flush()

    key_ids = reward_issuance_task_type.get_key_ids_by_name()
    db_session.add_all(
        [
            TaskTypeKeyValue(
                task_type_key_id=key_ids[key],
                value=value,
                retry_task_id=task.retry_task_id,
            )
            for key, value in reward_issuance_task_params.items()
        ]
    )
    db_session.commit()
    return task


@pytest.fixture(scope="function")
def issuance_retry_task_no_reward(
    db_session: "Session", reward_issuance_task_params_no_reward: dict, reward_issuance_task_type: TaskType
) -> RetryTask:
    task = RetryTask(task_type_id=reward_issuance_task_type.task_type_id)
    db_session.add(task)
    db_session.flush()

    key_ids = reward_issuance_task_type.get_key_ids_by_name()
    db_session.add_all(
        [
            TaskTypeKeyValue(
                task_type_key_id=key_ids[key],
                value=value,
                retry_task_id=task.retry_task_id,
            )
            for key, value in reward_issuance_task_params_no_reward.items()
        ]
    )
    db_session.commit()
    return task


@pytest.fixture(scope="function")
def issuance_expected_payload(reward_issuance_task_params: dict) -> dict:
    return {
        "code": reward_issuance_task_params["code"],
        "reward_slug": reward_issuance_task_params["reward_slug"],
        "reward_uuid": reward_issuance_task_params["reward_uuid"],
        "associated_url": "",
    }


@pytest.fixture(scope="function")
def reward_update(db_session: "Session", reward: Reward) -> RewardUpdate:
    adjustment = RewardUpdate(
        reward=reward,
        date=datetime.now(tz=timezone.utc).date(),
        status=RewardUpdateStatuses.REDEEMED,
    )
    db_session.add(adjustment)
    db_session.commit()
    return adjustment


@pytest.fixture(scope="function")
def reward_status_adjustment_task_params(reward_update: RewardUpdate) -> dict:
    return {
        "reward_uuid": str(reward_update.reward_uuid),
        "retailer_slug": reward_update.reward.retailer.slug,
        "date": str(datetime.fromisoformat(reward_update.date.isoformat()).replace(tzinfo=timezone.utc).timestamp()),
        "status": reward_update.status.name,
    }


@pytest.fixture(scope="function")
def reward_status_adjustment_retry_task(
    db_session: "Session", reward_status_adjustment_task_params: dict, reward_status_adjustment_task_type: TaskType
) -> RetryTask:
    task = RetryTask(task_type_id=reward_status_adjustment_task_type.task_type_id)
    db_session.add(task)
    db_session.flush()

    key_ids = reward_status_adjustment_task_type.get_key_ids_by_name()
    db_session.add_all(
        [
            TaskTypeKeyValue(
                task_type_key_id=key_ids[key],
                value=value,
                retry_task_id=task.retry_task_id,
            )
            for key, value in reward_status_adjustment_task_params.items()
        ]
    )
    db_session.commit()
    return task


@pytest.fixture(scope="function")
def adjustment_expected_payload(reward_status_adjustment_retry_task: RetryTask) -> dict:
    params = reward_status_adjustment_retry_task.get_params()
    return {
        "status": params["status"],
        "date": params["date"],
    }


@pytest.fixture(scope="function")
def adjustment_url(reward_status_adjustment_task_params: dict) -> str:
    return "{base_url}/{retailer_slug}/rewards/{reward_uuid}/status".format(
        base_url=settings.POLARIS_BASE_URL,
        retailer_slug=reward_status_adjustment_task_params["retailer_slug"],
        reward_uuid=reward_status_adjustment_task_params["reward_uuid"],
    )


@pytest.fixture(scope="function")
def delete_rewards_retry_task(
    db_session: "Session", reward_deletion_task_type: TaskType, reward_config: RewardConfig
) -> RetryTask:
    task = sync_create_task(
        db_session,
        task_type_name=reward_deletion_task_type.name,
        params={
            "retailer_id": reward_config.retailer_id,
            "reward_slug": reward_config.reward_slug,
        },
    )
    db_session.commit()
    return task


@pytest.fixture(scope="function")
def cancel_rewards_retry_task(
    db_session: "Session", reward_cancellation_task_type: TaskType, reward_config: RewardConfig
) -> RetryTask:
    task = sync_create_task(
        db_session,
        task_type_name=reward_cancellation_task_type.name,
        params={
            "retailer_slug": reward_config.retailer.slug,
            "reward_slug": reward_config.reward_slug,
        },
    )
    db_session.commit()
    return task


@pytest.fixture(scope="function")
def create_reward_config(db_session: "Session", pre_loaded_retailer_fetch_type: RetailerFetchType) -> Callable:
    def _create_reward_config(**reward_config_params: Any) -> RewardConfig:
        mock_reward_config_params = {
            "reward_slug": "test-reward",
            "required_fields_values": "validity_days: 15",
            "retailer_id": pre_loaded_retailer_fetch_type.retailer_id,
            "fetch_type_id": pre_loaded_retailer_fetch_type.fetch_type_id,
            "status": RewardTypeStatuses.ACTIVE,
        }

        mock_reward_config_params.update(reward_config_params)
        reward_config = RewardConfig(**mock_reward_config_params)
        db_session.add(reward_config)
        db_session.commit()

        return reward_config

    return _create_reward_config
