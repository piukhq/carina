import asyncio

from typing import Any

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.api.deps import get_session, retailer_is_valid, user_is_authorised
from app.api.tasks import enqueue_many_tasks, enqueue_task
from app.db.base_class import async_run_query
from app.enums import HttpErrors, RewardTypeStatuses
from app.fetch_reward import get_allocable_reward
from app.models import Retailer
from app.schemas import RewardAllocationSchema
from app.schemas.reward import RewardStatusSchema

router = APIRouter()


@router.post(
    path="/{retailer_slug}/rewards/{reward_slug}/allocation",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(user_is_authorised)],
)
async def allocation(
    payload: RewardAllocationSchema,
    reward_slug: str,
    retailer: Retailer = Depends(retailer_is_valid),
    db_session: AsyncSession = Depends(get_session),
) -> Any:
    reward_config = await crud.get_reward_config(db_session, retailer, reward_slug)
    reward, issued, expiry = await get_allocable_reward(db_session, reward_config)
    retry_task = await crud.create_reward_issuance_retry_task(
        db_session,
        reward=reward,
        issued_date=issued,
        expiry_date=expiry,
        reward_config=reward_config,
        account_url=payload.account_url,
    )

    asyncio.create_task(enqueue_task(retry_task_id=retry_task.retry_task_id))
    return {}


@router.patch(
    path="/{retailer_slug}/rewards/{reward_slug}/status",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(user_is_authorised)],
)
async def reward_type_status(
    payload: RewardStatusSchema,
    reward_slug: str,
    retailer: Retailer = Depends(retailer_is_valid),
    db_session: AsyncSession = Depends(get_session),
) -> Any:
    reward_config = await crud.get_reward_config(db_session, retailer, reward_slug, for_update=True)

    if reward_config.status != RewardTypeStatuses.ACTIVE:  # pragma: coverage bug 1012
        raise HttpErrors.STATUS_UPDATE_FAILED.value

    async def _query() -> None:  # pragma: coverage bug 1012
        reward_config.status = payload.status
        return await db_session.commit()

    await async_run_query(_query, db_session)

    retry_tasks_ids = await crud.create_delete_and_cancel_rewards_tasks(
        db_session,
        retailer=retailer,
        reward_slug=reward_slug,
        create_cancel_task=payload.status == RewardTypeStatuses.CANCELLED,
    )

    asyncio.create_task(enqueue_many_tasks(retry_tasks_ids=retry_tasks_ids))
    return {}
