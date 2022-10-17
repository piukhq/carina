# pylint: disable=invalid-name

import uuid

from collections import namedtuple
from typing import TYPE_CHECKING, Callable, Generator

import pytest

from retry_tasks_lib.db.models import TaskType, TaskTypeKey
from sqlalchemy_utils import create_database, database_exists, drop_database
from testfixtures import LogCapture

from carina.core.config import redis, settings
from carina.db.base import Base
from carina.db.session import SyncSessionMaker, sync_engine
from carina.enums import RewardCampaignStatuses, RewardTypeStatuses
from carina.models import FetchType, Retailer, Reward, RewardCampaign, RewardConfig
from carina.models.retailer import RetailerFetchType
from carina.tasks.error_handlers import default_handler, handle_retry_task_request_error
from carina.tasks.issuance import issue_reward
from carina.tasks.status_adjustment import status_adjustment

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

SetupType = namedtuple("SetupType", ["db_session", "reward_config", "reward"])

# Top-level conftest for tests, doing things like setting up DB


def _get_path(fun: Callable) -> str:
    return fun.__module__ + "." + fun.__name__


@pytest.fixture(scope="session", autouse=True)
def setup_db() -> Generator:
    if sync_engine.url.database != "carina_test":
        raise ValueError(f"Unsafe attempt to recreate database: {sync_engine.url.database}")

    if database_exists(sync_engine.url):
        drop_database(sync_engine.url)
    create_database(sync_engine.url)

    yield

    # At end of all tests, drop the test db
    drop_database(sync_engine.url)


@pytest.fixture(scope="session", autouse=True)
def setup_redis() -> Generator:

    yield

    # At end of all tests, delete the tasks from the queue
    redis.flushdb()


@pytest.fixture(scope="session")
def main_db_session() -> Generator["Session", None, None]:
    with SyncSessionMaker() as session:
        yield session


@pytest.fixture(scope="function")
def db_session(main_db_session: "Session") -> Generator["Session", None, None]:
    yield main_db_session
    main_db_session.rollback()
    main_db_session.expunge_all()


@pytest.fixture(scope="function", autouse=True)
def setup_tables() -> Generator:
    """
    autouse set to True so will be run before each test function, to set up tables
    and tear them down after each test runs
    """
    Base.metadata.create_all(bind=sync_engine)

    yield

    # Drop all tables after each test
    Base.metadata.drop_all(bind=sync_engine)


@pytest.fixture(scope="function")
def setup(db_session: "Session", reward_config: RewardConfig, reward: Reward) -> Generator[SetupType, None, None]:
    yield SetupType(db_session, reward_config, reward)


@pytest.fixture(scope="function")
def retailer(db_session: "Session") -> Retailer:
    r = Retailer(slug="test-retailer")
    db_session.add(r)
    db_session.commit()
    return r


@pytest.fixture(scope="function")
def pre_loaded_fetch_type(db_session: "Session") -> FetchType:
    ft = FetchType(
        name="PRE_LOADED",
        required_fields="validity_days: integer",
        path="carina.fetch_reward.pre_loaded.PreLoaded",
    )
    db_session.add(ft)
    db_session.commit()
    return ft


@pytest.fixture(scope="function")
def jigsaw_fetch_type(db_session: "Session") -> FetchType:
    ft = FetchType(
        name="JIGSAW_EGIFT",
        path="carina.fetch_reward.jigsaw.Jigsaw",
        required_fields="transaction_value: integer",
    )
    db_session.add(ft)
    db_session.commit()
    return ft


@pytest.fixture(scope="function")
def jigsaw_retailer_fetch_type(
    db_session: "Session", retailer: Retailer, jigsaw_fetch_type: FetchType
) -> RetailerFetchType:
    rft = RetailerFetchType(
        retailer_id=retailer.id,
        fetch_type_id=jigsaw_fetch_type.id,
        agent_config='base_url: "http://test.url"\n' "brand_id: 30\n" "fetch_reward: true\n" 'fetch_balance: false"',
    )
    db_session.add(rft)
    db_session.commit()
    return rft


@pytest.fixture(scope="function")
def pre_loaded_retailer_fetch_type(
    db_session: "Session", retailer: Retailer, pre_loaded_fetch_type: FetchType
) -> RetailerFetchType:
    rft = RetailerFetchType(
        retailer_id=retailer.id,
        fetch_type_id=pre_loaded_fetch_type.id,
    )
    db_session.add(rft)
    db_session.commit()
    return rft


@pytest.fixture(scope="function")
def reward_config(db_session: "Session", pre_loaded_retailer_fetch_type: RetailerFetchType) -> RewardConfig:
    config = RewardConfig(
        reward_slug="test-reward",
        required_fields_values="validity_days: 15",
        retailer_id=pre_loaded_retailer_fetch_type.retailer_id,
        fetch_type_id=pre_loaded_retailer_fetch_type.fetch_type_id,
        status=RewardTypeStatuses.ACTIVE,
    )
    db_session.add(config)
    db_session.commit()
    return config


@pytest.fixture(scope="function")
def jigsaw_reward_config(db_session: "Session", jigsaw_retailer_fetch_type: RetailerFetchType) -> RewardConfig:
    config = RewardConfig(
        reward_slug="test-jigsaw-reward",
        required_fields_values="transaction_value: 15",
        retailer_id=jigsaw_retailer_fetch_type.retailer_id,
        fetch_type_id=jigsaw_retailer_fetch_type.fetch_type_id,
        status=RewardTypeStatuses.ACTIVE,
    )
    db_session.add(config)
    db_session.commit()
    return config


@pytest.fixture(scope="function")
def reward(db_session: "Session", reward_config: RewardConfig) -> Reward:
    rc = Reward(
        code="TSTCD1234",
        retailer_id=reward_config.retailer_id,
        reward_config=reward_config,
    )
    db_session.add(rc)
    db_session.commit()
    return rc


@pytest.fixture(scope="function")
def create_reward(db_session: "Session", reward_config: RewardConfig) -> Callable:
    def _create_reward(**reward_params: dict) -> Reward:
        """
        Create a reward in the test DB
        :param reward_params: override any default values for the reward
        :return: Callable function
        """
        mock_reward_params = {
            "code": "TSTCD1234",
            "retailer_id": reward_config.retailer_id,
            "reward_config": reward_config,
        }

        mock_reward_params.update(reward_params)
        rwd = Reward(**mock_reward_params)
        db_session.add(rwd)
        db_session.commit()

        return rwd

    return _create_reward


@pytest.fixture()
def create_rewards(db_session: "Session", reward_config: RewardConfig) -> Callable:
    def fn(override_datas: list[dict]) -> dict[str, Reward]:
        reward_data = {
            "code": str(uuid.uuid4()),
            "deleted": False,
            "allocated": False,
            "reward_config_id": reward_config.id,
            "retailer_id": reward_config.retailer_id,
        }
        rewards = [Reward(**reward_data | override_data) for override_data in override_datas]
        db_session.add_all(rewards)
        db_session.commit()
        return {reward.code: reward for reward in rewards}

    return fn


@pytest.fixture(scope="function")
def reward_campaign(db_session: "Session", reward_config: RewardConfig, retailer: Retailer) -> RewardCampaign:
    rc = RewardCampaign(
        reward_slug=reward_config.reward_slug,
        campaign_slug="test-campaign",
        retailer_id=retailer.id,
        campaign_status=RewardCampaignStatuses.ACTIVE,
    )
    db_session.add(rc)
    db_session.commit()
    return rc


@pytest.fixture(scope="function")
def capture() -> Generator:
    with LogCapture() as cpt:
        yield cpt


@pytest.fixture(scope="function")
def reward_issuance_task_type(db_session: "Session") -> TaskType:
    task = TaskType(
        name=settings.REWARD_ISSUANCE_TASK_NAME,
        path=_get_path(issue_reward),
        queue_name="carina:default",
        error_handler_path=_get_path(handle_retry_task_request_error),
    )
    db_session.add(task)
    db_session.flush()

    db_session.bulk_save_objects(
        [
            TaskTypeKey(task_type_id=task.task_type_id, name=key_name, type=key_type)
            for key_name, key_type in (
                ("account_url", "STRING"),
                ("issued_date", "FLOAT"),
                ("expiry_date", "FLOAT"),
                ("reward_config_id", "INTEGER"),
                ("reward_slug", "STRING"),
                ("reward_uuid", "STRING"),
                ("code", "STRING"),
                ("idempotency_token", "STRING"),
                ("agent_state_params_raw", "STRING"),
                ("pending_reward_id", "STRING"),
                ("retailer_slug", "STRING"),
                ("campaign_slug", "STRING"),
            )
        ]
    )

    db_session.commit()
    return task


@pytest.fixture(scope="function")
def reward_status_adjustment_task_type(db_session: "Session") -> TaskType:
    task = TaskType(
        name=settings.REWARD_STATUS_ADJUSTMENT_TASK_NAME,
        path=_get_path(status_adjustment),
        queue_name="carina:default",
        error_handler_path=_get_path(handle_retry_task_request_error),
    )
    db_session.add(task)
    db_session.flush()

    db_session.bulk_save_objects(
        [
            TaskTypeKey(task_type_id=task.task_type_id, name=key_name, type=key_type)
            for key_name, key_type in (
                ("reward_uuid", "STRING"),
                ("retailer_slug", "STRING"),
                ("date", "FLOAT"),
                ("status", "STRING"),
            )
        ]
    )

    db_session.commit()
    return task


@pytest.fixture(scope="function")
def reward_deletion_task_type(db_session: "Session") -> TaskType:
    task = TaskType(
        name=settings.DELETE_UNALLOCATED_REWARDS_TASK_NAME,
        path=_get_path(issue_reward),
        queue_name="carina:default",
        error_handler_path=_get_path(default_handler),
    )
    db_session.add(task)
    db_session.flush()

    db_session.bulk_save_objects(
        [
            TaskTypeKey(task_type_id=task.task_type_id, name=key_name, type=key_type)
            for key_name, key_type in (
                ("reward_slug", "STRING"),
                ("retailer_id", "INTEGER"),
            )
        ]
    )

    db_session.commit()
    return task


@pytest.fixture(scope="function")
def reward_cancellation_task_type(db_session: "Session") -> TaskType:
    task = TaskType(
        name=settings.CANCEL_REWARDS_TASK_NAME,
        path=_get_path(issue_reward),
        queue_name="carina:default",
        error_handler_path=_get_path(handle_retry_task_request_error),
    )
    db_session.add(task)
    db_session.flush()

    db_session.bulk_save_objects(
        [
            TaskTypeKey(task_type_id=task.task_type_id, name=key_name, type=key_type)
            for key_name, key_type in (
                ("reward_slug", "STRING"),
                ("retailer_slug", "STRING"),
            )
        ]
    )

    db_session.commit()
    return task


@pytest.fixture
def run_task_with_metrics() -> Generator:
    val = getattr(settings, "ACTIVATE_TASKS_METRICS")
    setattr(settings, "ACTIVATE_TASKS_METRICS", True)
    yield
    setattr(settings, "ACTIVATE_TASKS_METRICS", val)
