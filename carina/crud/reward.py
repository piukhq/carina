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
from sqlalchemy.orm import noload, selectinload

from carina.core.config import settings
from carina.db.base_class import async_run_query
from carina.enums import HttpErrors, RewardCampaignStatuses
from carina.models import (
    CAMPAIGN_RETAILER_UNQ_CONSTRAINT_NAME,
    IDEMPOTENCY_TOKEN_REWARD_ALLOCATION_UNQ_CONSTRAINT_NAME,
    Allocation,
    Retailer,
    RewardCampaign,
    RewardConfig,
)

logger = logging.getLogger("reward-crud")


async def get_reward_config(
    db_session: AsyncSession,
    retailer: Retailer,
    reward_slug: str,
    for_update: bool = False,
) -> RewardConfig:
    async def _query() -> list[RewardConfig]:
        option = selectinload if not for_update else noload
        stmt = (
            select(RewardConfig)
            .options(option(RewardConfig.fetch_type))
            .where(RewardConfig.retailer_id == retailer.id, RewardConfig.reward_slug == reward_slug)
        )
        if for_update:
            stmt = stmt.with_for_update()

        return (await db_session.execute(stmt)).unique().scalar_one_or_none()

    reward_config = await async_run_query(_query, db_session)
    if reward_config is None:
        raise HttpErrors.UNKNOWN_REWARD_SLUG.value

    return reward_config


async def create_reward_issuance_retry_tasks(
    db_session: AsyncSession,
    *,
    reward_config: RewardConfig,
    retailer_slug: str,
    campaign_slug: str | None,
    account_url: str,
    count: int,
    idempotency_token: UUID,
    pending_reward_id: uuid.UUID | None,
    reason: str | None,
) -> tuple[int, list[int]]:
    async def _query() -> tuple[int, list[int]]:
        task_name = settings.REWARD_ISSUANCE_TASK_NAME
        reward_slug = reward_config.reward_slug
        task_params = {
            "account_url": account_url,
            "reward_config_id": reward_config.id,
            "reward_slug": reward_config.reward_slug,
            "retailer_slug": retailer_slug,
            "reason": reason,
        }
        if pending_reward_id is not None:
            task_params["pending_reward_id"] = pending_reward_id
        if campaign_slug is not None:
            task_params["campaign_slug"] = campaign_slug

        reward_issuance_tasks = []
        status_code = http_status.HTTP_202_ACCEPTED

        try:
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


async def insert_or_update_reward_campaign(
    db_session: AsyncSession,
    *,
    reward_slug: str,
    retailer_id: int,
    campaign_slug: str,
    campaign_status: RewardCampaignStatuses,
) -> int:
    async def _query() -> int:
        status_code = http_status.HTTP_201_CREATED
        try:
            new_reward_campaign = RewardCampaign(
                reward_slug=reward_slug,
                campaign_slug=campaign_slug,
                retailer_id=retailer_id,
                campaign_status=campaign_status,
            )
            db_session.add(new_reward_campaign)
            await db_session.commit()
        except IntegrityError as ex:
            # change this to use ex.orig.diag.constraint_name when we migrate to psycopg3
            if CAMPAIGN_RETAILER_UNQ_CONSTRAINT_NAME not in ex.args[0]:
                raise

            await db_session.rollback()
            existing_reward_campaign: RewardCampaign = (
                await db_session.execute(
                    select(RewardCampaign).where(
                        RewardCampaign.campaign_slug == campaign_slug, RewardCampaign.retailer_id == retailer_id
                    )
                )
            ).scalar_one()
            existing_reward_campaign.campaign_status = campaign_status
            await db_session.commit()
            status_code = http_status.HTTP_200_OK

        return status_code  # pylint: disable=lost-exception

    return await async_run_query(_query, db_session)


async def check_for_active_campaigns(
    db_session: AsyncSession,
    retailer: Retailer,
    reward_slug: str,
) -> RewardCampaign | None:
    async def _query() -> RewardCampaign | None:
        campaigns = select(RewardCampaign).where(
            RewardCampaign.retailer_id == retailer.id,
            RewardCampaign.reward_slug == reward_slug,
            RewardCampaign.campaign_status == RewardCampaignStatuses.ACTIVE,
        )

        return (await db_session.execute(campaigns)).scalar_one_or_none()

    return await async_run_query(_query, db_session)
