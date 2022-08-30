from uuid import uuid4

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.utils.asynchronous import async_create_task
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.core.config import settings
from app.db.base_class import async_run_query
from app.enums import HttpErrors
from app.models import Retailer, RewardConfig


async def get_reward_config(
    db_session: AsyncSession,
    retailer: Retailer,
    reward_slug: str,
    for_update: bool = False,
) -> RewardConfig:
    async def _query() -> list[RewardConfig]:
        stmt = select(RewardConfig).where(
            RewardConfig.retailer_id == retailer.id, RewardConfig.reward_slug == reward_slug
        )
        if for_update:
            stmt = stmt.with_for_update()

        return (await db_session.execute(stmt)).scalar_one_or_none()

    reward_config = await async_run_query(_query, db_session)
    if reward_config is None:
        raise HttpErrors.UNKNOWN_REWARD_SLUG.value

    return reward_config


async def _create_retry_task(db_session: AsyncSession, task_type_name: str, task_params: dict) -> RetryTask:
    async def _query() -> RetryTask:
        retry_task = await async_create_task(db_session=db_session, task_type_name=task_type_name, params=task_params)
        await db_session.commit()
        return retry_task

    return await async_run_query(_query, db_session)


async def create_reward_issuance_retry_tasks(
    db_session: AsyncSession,
    *,
    reward_config: RewardConfig,
    account_url: str,
    count: int,
) -> list[int]:
    task_name = settings.REWARD_ISSUANCE_TASK_NAME
    task_params = {
        "account_url": account_url,
        "reward_config_id": reward_config.id,
        "reward_slug": reward_config.reward_slug,
    }

    reward_issuance_tasks = []
    for _ in range(count):
        task_params["idempotency_token"] = uuid4()
        reward_issuance_task = await _create_retry_task(db_session, task_type_name=task_name, task_params=task_params)
        reward_issuance_tasks.append(reward_issuance_task)

    return [task.retry_task_id for task in reward_issuance_tasks]


async def create_delete_and_cancel_rewards_tasks(
    db_session: AsyncSession, *, retailer: Retailer, reward_slug: str
) -> list[int]:
    async def _query() -> tuple[RetryTask, RetryTask | None]:
        delete_task: RetryTask = await async_create_task(
            db_session=db_session,
            task_type_name=settings.DELETE_UNALLOCATED_REWARDS_TASK_NAME,
            params={"retailer_id": retailer.id, "reward_slug": reward_slug},
        )
        cancel_task: RetryTask = await async_create_task(
            db_session=db_session,
            task_type_name=settings.CANCEL_REWARDS_TASK_NAME,
            params={"retailer_slug": retailer.slug, "reward_slug": reward_slug},
        )

        await db_session.commit()
        return delete_task, cancel_task

    return [task.retry_task_id for task in await async_run_query(_query, db_session)]
