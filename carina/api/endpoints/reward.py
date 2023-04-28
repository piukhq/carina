import asyncio
import logging

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from carina import crud
from carina.api.deps import get_idempotency_token, get_session, retailer_is_valid, user_is_authorised
from carina.api.tasks import enqueue_many_tasks
from carina.db.base_class import async_run_query
from carina.enums import HttpErrors, RewardFetchType, RewardTypeStatuses
from carina.models import Retailer, Reward
from carina.schemas import RewardAllocationSchema, RewardCampaignSchema

router = APIRouter()

logger = logging.getLogger("reward")


@router.post(
    path="/{retailer_slug}/rewards/{reward_slug}/allocation",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(user_is_authorised)],
)
async def allocation(  # noqa: PLR0913
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
        reason=payload.activity_metadata.reason if payload.activity_metadata else None,
    )

    if reward_issuance_task_ids:
        asyncio.create_task(enqueue_many_tasks(retry_tasks_ids=reward_issuance_task_ids))

    return {}


@router.put(
    path="/{retailer_slug}/{reward_slug}/campaign",
    dependencies=[Depends(user_is_authorised)],
)
async def reward_campaign(
    payload: RewardCampaignSchema,
    response: Response,
    reward_slug: str,
    retailer: Retailer = Depends(retailer_is_valid),
    db_session: AsyncSession = Depends(get_session),
) -> Any:
    reward_config = await crud.get_reward_config(db_session, retailer, reward_slug, for_update=True)

    if reward_config.status != RewardTypeStatuses.ACTIVE:
        raise HttpErrors.UNKNOWN_REWARD_SLUG.value

    response.status_code = await crud.insert_or_update_reward_campaign(
        db_session,
        reward_slug=reward_config.reward_slug,
        retailer_id=retailer.id,
        campaign_slug=payload.campaign_slug,
        campaign_status=payload.status,
    )

    return {}


@router.delete(
    path="/{retailer_slug}/rewards/{reward_slug}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(user_is_authorised)],
)
async def deactivate_reward_type(
    reward_slug: str,
    retailer: Retailer = Depends(retailer_is_valid),
    db_session: AsyncSession = Depends(get_session),
) -> None:
    reward_config = await crud.get_reward_config(db_session, retailer, reward_slug)
    active_campaign = await crud.check_for_active_campaigns(db_session, retailer, reward_slug)
    if active_campaign:
        raise HttpErrors.DELETE_FAILED.value

    async def _query() -> None:
        reward_config.status = RewardTypeStatuses.DELETED
        if reward_config.fetch_type.name == RewardFetchType.PRE_LOADED.name:
            result = await db_session.execute(
                Reward.__table__.delete().where(
                    Reward.allocated.is_(False),
                    Reward.retailer_id == retailer.id,
                    Reward.reward_config_id == reward_config.id,
                )
            )
            logger.info(f"Deleted {result.rowcount} unallocated reward rows for {reward_config}")
        return await db_session.commit()

    await async_run_query(_query, db_session)
