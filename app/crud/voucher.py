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
    db_session: AsyncSession,
    voucher: Optional[Voucher],
    issued_date: float,
    expiry_date: float,
    voucher_config: VoucherConfig,
    account_url: str,
) -> VoucherAllocation:
    async def _query() -> VoucherAllocation:
        if voucher is not None:
            voucher.allocated = True

        allocation = VoucherAllocation(
            voucher=voucher,
            voucher_config=voucher_config,
            account_url=account_url,
            issued_date=issued_date,
            expiry_date=expiry_date,
        )
        db_session.add(allocation)
        await db_session.commit()
        return allocation

    return await async_run_query(_query, db_session)
