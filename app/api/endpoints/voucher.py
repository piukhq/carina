import asyncio

from typing import Any

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.api.deps import get_session, user_is_authorised
from app.fetch_voucher import get_allocable_voucher
from app.schemas import VoucherAllocationSchema
from app.tasks.voucher import enqueue_voucher_allocation

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
    voucher_allocation = await crud.create_allocation(
        db_session, voucher, issued, expiry, voucher_config, payload.account_url
    )

    asyncio.create_task(enqueue_voucher_allocation(voucher_allocation.id))
    return {}
