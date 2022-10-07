import json

from importlib import import_module
from typing import TYPE_CHECKING, Type

from sqlalchemy.future import select

from carina.db.base_class import sync_run_query
from carina.models import RetailerFetchType, RewardConfig

from .base import BaseAgent, RewardData

if TYPE_CHECKING:  # pragma: no cover
    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session


def _reward_config_specific_agent(
    db_session: "Session", reward_config: RewardConfig, retry_task: "RetryTask"
) -> BaseAgent:
    try:
        mod, cls = reward_config.fetch_type.path.rsplit(".", 1)
        mod = import_module(mod)
        Agent: Type[BaseAgent] = getattr(mod, cls)  # pylint: disable=invalid-name
    except (ValueError, ModuleNotFoundError, AttributeError) as ex:
        BaseAgent.logger.warning(
            f"Could not import agent class for fetch_type {reward_config.fetch_type.name}.", exc_info=ex
        )
        raise

    def _query() -> RetailerFetchType:
        return db_session.execute(
            select(RetailerFetchType).where(
                RetailerFetchType.retailer_id == reward_config.retailer_id,
                RetailerFetchType.fetch_type_id == reward_config.fetch_type_id,
            )
        ).scalar_one()

    agent_config: dict = sync_run_query(_query, db_session).load_agent_config()
    return Agent(db_session, reward_config, agent_config, retry_task=retry_task)


def get_allocable_reward(db_session: "Session", reward_config: RewardConfig, retry_task: "RetryTask") -> RewardData:
    with _reward_config_specific_agent(db_session, reward_config, retry_task) as agent:
        return agent.fetch_reward()


def cleanup_reward(db_session: "Session", reward_config: RewardConfig, retry_task: "RetryTask") -> None:
    with _reward_config_specific_agent(db_session, reward_config, retry_task) as agent:
        return agent.cleanup()


def get_associated_url(task_params: dict) -> str:
    associated_url: str = ""
    if BaseAgent.AGENT_STATE_PARAMS_RAW_KEY in task_params:
        associated_url = json.loads(task_params[BaseAgent.AGENT_STATE_PARAMS_RAW_KEY]).get(
            BaseAgent.ASSOCIATED_URL_KEY, ""
        )

    return associated_url
