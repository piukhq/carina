from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.db.base_class import async_run_query
from app.enums import HttpErrors
from app.models import Voucher, VoucherAllocation, VoucherConfig


async def get_voucher_config(db_session: AsyncSession, retailer_slug: str, voucher_type_slug: str) -> VoucherConfig:
    async def _query() -> List[VoucherConfig]:
        return (await db_session.execute(select(VoucherConfig).filter_by(retailer_slug=retailer_slug))).scalars().all()

    retailer_vouchers = await async_run_query(_query, db_session)
    if not retailer_vouchers:
        raise HttpErrors.INVALID_RETAILER.value

    voucher_config = next(
        (voucher for voucher in retailer_vouchers if voucher.voucher_type_slug == voucher_type_slug), None
    )
    if voucher_config is None:
        raise HttpErrors.UNKNOWN_VOUCHER_TYPE.value

    return voucher_config


async def get_allocable_voucher(db_session: AsyncSession, voucher_config: VoucherConfig) -> Optional[Voucher]:
    async def _query() -> Voucher:
        return (
            await db_session.execute(
                select(Voucher).filter_by(
                    voucher_config_id=voucher_config.id,
                    allocated=False,
                )
            )
            .scalars()
            .first()
        )

    return await async_run_query(_query, db_session)


async def create_allocation(
    db_session: AsyncSession, voucher: Optional[Voucher], voucher_config: VoucherConfig, account_url: str
) -> VoucherAllocation:
    now = datetime.utcnow()
    voucher_id = None
    expiry = None
    issued = now.timestamp()

    if voucher is not None:
        voucher_id = voucher.id
        expiry = (now + timedelta(days=voucher.voucher_config.validity_days)).timestamp()

    async def _query() -> VoucherAllocation:
        if voucher is not None:
            voucher.allocated = True

        allocation = VoucherAllocation(
            voucher_id=voucher_id,
            voucher_config=voucher_config,
            account_url=account_url,
            issued_date=issued,
            expiry_date=expiry,
        )
        db_session.add(allocation)
        await db_session.commit()
        return allocation

    return await async_run_query(_query, db_session)
