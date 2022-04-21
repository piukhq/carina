import json
import logging

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from retry_tasks_lib.db.models import TaskTypeKey, TaskTypeKeyValue
from sqlalchemy.future import select

from app.db.base_class import sync_run_query
from app.tasks import send_request_with_metrics

if TYPE_CHECKING:  # pragma: no cover
    from inspect import Traceback

    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session

    from app.models import RewardConfig


class BaseAgent(ABC):
    logger = logging.getLogger("agents")

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
                    TaskTypeKey.name == "agent_state_params_raw",
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

    def set_agent_state_params(self, value: dict) -> None:
        def _query(val: str) -> None:
            if self._agent_state_params_raw_instance is None:
                self._agent_state_params_raw_instance = TaskTypeKeyValue(
                    retry_task_id=self.retry_task.retry_task_id,
                    value=val,
                    task_type_key_id=self.db_session.execute(
                        select(TaskTypeKey.task_type_key_id).where(
                            TaskTypeKey.task_type_id == self.retry_task.task_type_id,
                            TaskTypeKey.name == "agent_state_params_raw",
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
    def fetch_reward(self) -> Any:  # pragma: no cover
        ...

    @abstractmethod
    def fetch_balance(self) -> Any:  # pragma: no cover
        ...


class AgentError(Exception):
    pass
