import json
import logging

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from retry_tasks_lib.db.models import TaskTypeKey, TaskTypeKeyValue
from sqlalchemy.future import select

from carina.db.base_class import sync_run_query
from carina.models import Reward
from carina.tasks import send_request_with_metrics

if TYPE_CHECKING:  # pragma: no cover
    from inspect import Traceback

    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session

    from carina.models import RewardConfig


@dataclass
class RewardData:
    reward: Reward | None
    issued_date: float | None
    expiry_date: float | None
    validity_days: int | None


class BaseAgent(ABC):
    logger = logging.getLogger("agents")

    ASSOCIATED_URL_KEY = "associated_url"
    AGENT_STATE_PARAMS_RAW_KEY = "agent_state_params_raw"

    def __init__(
        self, db_session: "Session", reward_config: "RewardConfig", config: dict, *, retry_task: "RetryTask"
    ) -> None:
        self.db_session = db_session
        self.reward_config = reward_config
        self.config = config
        self.retry_task = retry_task
        self.send_request = send_request_with_metrics
        self._agent_state_params_raw_instance: TaskTypeKeyValue
        self.agent_state_params: dict
        self._load_agent_state_params_raw_instance()

    def __enter__(self) -> "BaseAgent":
        return self

    def __exit__(self, exc_type: type, exc_value: Exception, exc_traceback: "Traceback") -> None:
        pass

    def _load_agent_state_params_raw_instance(self) -> None:
        def _query() -> TaskTypeKeyValue:
            return self.db_session.scalar(
                select(TaskTypeKeyValue).where(
                    TaskTypeKeyValue.retry_task_id == self.retry_task.retry_task_id,
                    TaskTypeKeyValue.task_type_key_id == TaskTypeKey.task_type_key_id,
                    TaskTypeKey.task_type_id == self.retry_task.task_type_id,
                    TaskTypeKey.name == self.AGENT_STATE_PARAMS_RAW_KEY,
                )
            )

        try:
            self._agent_state_params_raw_instance = sync_run_query(_query, self.db_session, rollback_on_exc=False)
            if self._agent_state_params_raw_instance is None:
                self.agent_state_params = {}
            else:
                self.agent_state_params = json.loads(self._agent_state_params_raw_instance.value)
        except Exception as ex:
            raise AgentError(
                "Error while loading the agent_state_params_raw TaskTypeValue "
                f"for RetryTask: {self.retry_task.retry_task_id}."
            ) from ex

    def _delete_task_params_by_key_names(self, key_names: list[str]) -> None:
        self.db_session.execute(
            TaskTypeKeyValue.__table__.delete().where(
                TaskTypeKeyValue.retry_task_id == self.retry_task.retry_task_id,
                TaskTypeKeyValue.task_type_key_id == TaskTypeKey.task_type_key_id,
                TaskTypeKey.name.in_(key_names),
            )
        )

    def update_reward_and_remove_references_from_task(self, reward_uuid: str, update_values: dict) -> None:
        def _query() -> None:
            self.db_session.execute(
                Reward.__table__.update()
                .values(**update_values)
                .where(
                    Reward.id == reward_uuid,
                    Reward.allocated.is_(True),
                    Reward.deleted.is_(False),
                )
            )
            self._delete_task_params_by_key_names(["reward_uuid", "code", "issued_date", "expiry_date"])
            self.db_session.commit()

        sync_run_query(_query, self.db_session)

    def _remove_reward_references_from_task_params(self) -> None:
        self._delete_task_params_by_key_names(["reward_uuid", "code", "issued_date", "expiry_date"])

    def set_agent_state_params(self, value: dict) -> None:
        def _query(val: str) -> None:
            if self._agent_state_params_raw_instance is None:
                self._agent_state_params_raw_instance = TaskTypeKeyValue(
                    retry_task_id=self.retry_task.retry_task_id,
                    value=val,
                    task_type_key_id=self.db_session.execute(
                        select(TaskTypeKey.task_type_key_id).where(
                            TaskTypeKey.task_type_id == self.retry_task.task_type_id,
                            TaskTypeKey.name == self.AGENT_STATE_PARAMS_RAW_KEY,
                        )
                    ).scalar_one(),
                )
                self.db_session.add(self._agent_state_params_raw_instance)
            else:
                self._agent_state_params_raw_instance.value = val

            self.db_session.commit()

        try:
            self.agent_state_params = value
            parsed_val = json.dumps(value)
            sync_run_query(_query, self.db_session, val=parsed_val)
        except Exception as ex:
            raise AgentError(
                "Error while saving the agent_state_params_raw TaskTypeValue "
                f"for RetryTask: {self.retry_task.retry_task_id}."
            ) from ex

    @abstractmethod
    def fetch_reward(self) -> RewardData:
        ...

    @abstractmethod
    def cleanup_reward(self) -> None:
        """
        Deletes all references to a Reward from the provided RetryTask and enables reallocation of said Reward's code.
        """

    @abstractmethod
    def fetch_balance(self) -> Any:  # pragma: no cover
        ...


class AgentError(Exception):
    pass
