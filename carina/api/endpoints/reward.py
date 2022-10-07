import asyncio

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from carina import crud
from carina.api.deps import get_idempotency_token, get_session, retailer_is_valid, user_is_authorised
from carina.api.tasks import enqueue_many_tasks
from carina.db.base_class import async_run_query
from carina.enums import HttpErrors, RewardTypeStatuses
from carina.models import Retailer
from carina.schemas import RewardAllocationSchema
from carina.schemas.reward import RewardStatusSchema

router = APIRouter()


@router.post(
    path="/{retailer_slug}/rewards/{reward_slug}/allocation",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(user_is_authorised)],
)
async def allocation(  # pylint: disable=too-many-arguments
    payload: RewardAllocationSchema,
    response: Response,
    reward_slug: str,
    retailer: Retailer = Depends(retailer_is_valid),
    db_session: AsyncSession = Depends(get_session),
    idempotency_token: UUID = Depends(get_idempotency_token),
) -> Any:
    reward_config = await crud.get_reward_config(db_session, retailer, reward_slug)
    response.status_code, reward_issuance_task_ids = await crud.create_reward_issuance_retry_tasks(
        db_session,
        reward_config=reward_config,
        retailer_slug=retailer.slug,
        campaign_slug=payload.campaign_slug,
        account_url=payload.account_url,
        count=payload.count,
        idempotency_token=idempotency_token,
        pending_reward_id=payload.pending_reward_id,
    )

    if reward_issuance_task_ids:
        asyncio.create_task(enqueue_many_tasks(retry_tasks_ids=reward_issuance_task_ids))

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

    if reward_config.status != RewardTypeStatuses.ACTIVE:
        raise HttpErrors.STATUS_UPDATE_FAILED.value

    async def _query() -> None:
        reward_config.status = payload.status
        return await db_session.commit()

    await async_run_query(_query, db_session)

    if payload.status == RewardTypeStatuses.CANCELLED:
        retry_tasks_ids = await crud.create_delete_and_cancel_rewards_tasks(
            db_session,
            retailer=retailer,
            reward_slug=reward_slug,
        )

        asyncio.create_task(enqueue_many_tasks(retry_tasks_ids=retry_tasks_ids))

    return {}
