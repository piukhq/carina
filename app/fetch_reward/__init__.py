from importlib import import_module
from typing import TYPE_CHECKING, Callable, Optional, Tuple, Type

from sqlalchemy.future import select

from app.db.base_class import sync_run_query
from app.models import RetailerFetchType, Reward, RewardConfig

from .base import BaseAgent

if TYPE_CHECKING:  # pragma: no cover
    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session


def get_allocable_reward(
    db_session: "Session", reward_config: RewardConfig, send_request_fn: Callable = None, retry_task: "RetryTask" = None
) -> Tuple[Optional[Reward], float, float]:

    try:
        mod, cls = reward_config.fetch_type.path.rsplit(".", 1)
        mod = import_module(mod)
        Agent: Type[BaseAgent] = getattr(mod, cls)
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

    with Agent(db_session, reward_config, agent_config, send_request_fn, retry_task) as agent:
        reward, issued, expiry = agent.fetch_reward()

    return reward, issued, expiry
