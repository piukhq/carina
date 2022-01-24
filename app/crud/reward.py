from typing import List, Optional
from uuid import uuid4

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.utils.asynchronous import async_create_task
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.core.config import settings
from app.db.base_class import async_run_query
from app.enums import HttpErrors
from app.models import Voucher, VoucherConfig


async def get_reward_config(
    db_session: AsyncSession,
    retailer_slug: str,
    reward_slug: str,
    for_update: bool = False,
) -> VoucherConfig:
    async def _query(by_reward_slug: bool = False) -> List[VoucherConfig]:
        stmt = select(VoucherConfig).where(VoucherConfig.retailer_slug == retailer_slug)
        if by_reward_slug:
            stmt = stmt.where(VoucherConfig.voucher_type_slug == reward_slug)
            if for_update:
                stmt = stmt.with_for_update()
        return await db_session.execute(stmt)

    retailer_reward_configs = (await async_run_query(_query, db_session)).scalars().all()
    if not retailer_reward_configs:
        raise HttpErrors.INVALID_RETAILER.value

    reward_config = (await async_run_query(_query, db_session, by_reward_slug=True)).scalar_one_or_none()
    if reward_config is None:
        raise HttpErrors.UNKNOWN_REWARD_SLUG.value

    return reward_config


async def get_allocable_reward(db_session: AsyncSession, reward_config: VoucherConfig) -> Optional[Voucher]:
    async def _query() -> Optional[Voucher]:
        return (
            (
                await db_session.execute(
                    select(Voucher)
                    .with_for_update()
                    .where(
                        Voucher.voucher_config_id == reward_config.id,
                        Voucher.allocated == False,  # noqa
                        Voucher.deleted == False,  # noqa
                    )
                    .limit(1)
                )
            )
            .scalars()
            .first()
        )

    return await async_run_query(_query, db_session)


async def _create_retry_task(db_session: AsyncSession, task_type_name: str, task_params: dict) -> RetryTask:
    async def _query() -> RetryTask:
        retry_task = await async_create_task(db_session=db_session, task_type_name=task_type_name, params=task_params)
        await db_session.commit()
        return retry_task

    return await async_run_query(_query, db_session)


async def create_reward_issuance_retry_task(
    db_session: AsyncSession,
    *,
    reward: Optional[Voucher],
    issued_date: float,
    expiry_date: float,
    reward_config: VoucherConfig,
    account_url: str,
) -> RetryTask:

    task_params = {
        "account_url": account_url,
        "issued_date": issued_date,
        "expiry_date": expiry_date,
        "voucher_config_id": reward_config.id,
        "voucher_type_slug": reward_config.voucher_type_slug,
        "idempotency_token": uuid4(),
    }

    if reward is not None:
        reward.allocated = True
        task_params.update(
            {
                "voucher_id": reward.id,
                "voucher_code": reward.voucher_code,
            }
        )

    return await _create_retry_task(db_session, settings.REWARD_ISSUANCE_TASK_NAME, task_params)


async def create_delete_and_cancel_rewards_tasks(
    db_session: AsyncSession, *, retailer_slug: str, reward_slug: str, create_cancel_task: bool
) -> list[int]:
    task_params = {"retailer_slug": retailer_slug, "reward_slug": reward_slug}

    async def _query() -> tuple[RetryTask, Optional[RetryTask]]:
        delete_task: RetryTask = await async_create_task(
            db_session=db_session, task_type_name=settings.DELETE_UNALLOCATED_REWARDS_TASK_NAME, params=task_params
        )
        cancel_task: Optional[RetryTask] = (
            await async_create_task(
                db_session=db_session, task_type_name=settings.CANCEL_REWARDS_TASK_NAME, params=task_params
            )
            if create_cancel_task is True
            else None
        )

        await db_session.commit()
        return delete_task, cancel_task

    return [task.retry_task_id for task in await async_run_query(_query, db_session) if task is not None]
