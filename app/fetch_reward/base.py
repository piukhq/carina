import logging

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Optional

import requests

if TYPE_CHECKING:  # pragma: no cover
    from inspect import Traceback

    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session

    from app.models import RewardConfig


class BaseAgent(ABC):
    logger = logging.getLogger("agents")

    def __init__(
        self,
        db_session: "Session",
        reward_config: "RewardConfig",
        config: dict,
        send_request_fn: Callable = None,
        retry_task: "RetryTask" = None,
    ) -> None:
        self.db_session = db_session
        self.reward_config = reward_config
        self.config = config
        self.retry_task: Optional["RetryTask"] = retry_task

        if send_request_fn is None:
            self.send_request = requests.request
        else:
            self.send_request = send_request_fn

    def __enter__(self) -> "BaseAgent":
        return self

    def __exit__(self, exc_type: type, exc_value: Exception, exc_traceback: "Traceback") -> None:
        pass

    @abstractmethod
    def fetch_reward(self) -> Any:  # pragma: no cover
        ...

    @abstractmethod
    def fetch_balance(self) -> Any:  # pragma: no cover
        ...
