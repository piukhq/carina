import asyncio

from typing import Any

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.api.deps import get_session, user_is_authorised
from app.api.tasks import enqueue_many_tasks, enqueue_task
from app.db.base_class import async_run_query
from app.enums import HttpErrors, VoucherTypeStatuses
from app.fetch_voucher import get_allocable_voucher
from app.schemas import VoucherAllocationSchema
from app.schemas.voucher import VoucherStatusSchema

router = APIRouter()


@router.post(
    path="/{retailer_slug}/vouchers/{voucher_type_slug}/allocation",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(user_is_authorised)],
)
async def allocation(
    payload: VoucherAllocationSchema,
    retailer_slug: str,
    voucher_type_slug: str,
    db_session: AsyncSession = Depends(get_session),
) -> Any:
    voucher_config = await crud.get_voucher_config(db_session, retailer_slug, voucher_type_slug)
    voucher, issued, expiry = await get_allocable_voucher(db_session, voucher_config)
    retry_task = await crud.create_voucher_issuance_retry_task(
        db_session,
        voucher=voucher,
        issued_date=issued,
        expiry_date=expiry,
        voucher_config=voucher_config,
        account_url=payload.account_url,
    )

    asyncio.create_task(enqueue_task(retry_task_id=retry_task.retry_task_id))
    return {}


@router.patch(
    path="/{retailer_slug}/vouchers/{voucher_type_slug}/status",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(user_is_authorised)],
)
async def voucher_type_status(
    payload: VoucherStatusSchema,
    retailer_slug: str,
    voucher_type_slug: str,
    db_session: AsyncSession = Depends(get_session),
) -> Any:
    voucher_config = await crud.get_voucher_config(db_session, retailer_slug, voucher_type_slug, for_update=True)

    if voucher_config.status != VoucherTypeStatuses.ACTIVE:
        raise HttpErrors.STATUS_UPDATE_FAILED.value

    async def _query() -> None:
        voucher_config.status = payload.status
        return await db_session.commit()

    await async_run_query(_query, db_session)

    retry_tasks_ids = await crud.create_delete_and_cancel_vouchers_tasks(
        db_session,
        retailer_slug=retailer_slug,
        voucher_type_slug=voucher_type_slug,
        create_cancel_task=payload.status == VoucherTypeStatuses.CANCELLED,
    )

    asyncio.create_task(enqueue_many_tasks(retry_tasks_ids=retry_tasks_ids))
    return {}
