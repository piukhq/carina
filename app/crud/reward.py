import logging
import uuid

from uuid import UUID, uuid4

import sentry_sdk

from fastapi import status as http_status
from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.utils.asynchronous import async_create_task
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.core.config import settings
from app.db.base_class import async_run_query
from app.enums import HttpErrors
from app.models import IDEMPOTENCY_TOKEN_REWARD_ALLOCATION_UNQ_CONSTRAINT_NAME, Allocation, Retailer, RewardConfig

logger = logging.getLogger("reward-crud")


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


async def create_reward_issuance_retry_tasks(
    db_session: AsyncSession,
    *,
    reward_config: RewardConfig,
    retailer_slug: str,
    account_url: str,
    count: int,
    idempotency_token: UUID | None = None,
    pending_reward_id: uuid.UUID | None,
) -> tuple[int, list[int]]:
    async def _query() -> tuple[int, list[int]]:
        task_name = settings.REWARD_ISSUANCE_TASK_NAME
        reward_slug = reward_config.reward_slug
        task_params = {
            "account_url": account_url,
            "reward_config_id": reward_config.id,
            "reward_slug": reward_config.reward_slug,
        }
        if pending_reward_id is not None:
            task_params["pending_reward_id"] = pending_reward_id

        reward_issuance_tasks = []
        status_code = http_status.HTTP_202_ACCEPTED

        try:
            if idempotency_token:
                allocation_request = Allocation(
                    idempotency_token=str(idempotency_token), count=count, account_url=account_url
                )
                db_session.add(allocation_request)
            for _ in range(count):
                task_params["idempotency_token"] = uuid4()
                reward_issuance_task = await async_create_task(
                    db_session=db_session, task_type_name=task_name, params=task_params
                )
                reward_issuance_tasks.append(reward_issuance_task)
            await db_session.commit()
        except IntegrityError as ex:
            if IDEMPOTENCY_TOKEN_REWARD_ALLOCATION_UNQ_CONSTRAINT_NAME not in ex.args[0]:
                raise

            status_code = http_status.HTTP_202_ACCEPTED
            await db_session.rollback()
            existing_allocation_request_id = (
                await db_session.execute(
                    select(Allocation.id).where(Allocation.idempotency_token == str(idempotency_token))
                )
            ).scalar_one()
            message = (
                f"IntegrityError on reward allocation when creating reward issuance tasks "
                f"account url: {account_url} (retailer slug: {retailer_slug}).\n"
                f"New allocation request for (reward slug: {reward_slug}) is using a conflicting token "
                f"{idempotency_token} with existing allocation request of id: {existing_allocation_request_id}\n{ex}"
            )
            logger.error(message)
            with sentry_sdk.push_scope() as scope:
                scope.fingerprint = ["{{ default }}", "{{ message }}"]
                sentry_sdk.capture_message(message)

        return status_code, [task.retry_task_id for task in reward_issuance_tasks]

    return await async_run_query(_query, db_session)


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
