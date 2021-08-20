from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.db.base_class import async_run_query, sync_run_query
from app.enums import HttpErrors
from app.models import Voucher, VoucherAllocation, VoucherConfig

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


async def get_voucher_config(db_session: AsyncSession, retailer_slug: str, voucher_type_slug: str) -> VoucherConfig:
    async def _query() -> List[VoucherConfig]:
        return (await db_session.execute(select(VoucherConfig).filter_by(retailer_slug=retailer_slug))).scalars().all()

    retailer_voucher_configs = await async_run_query(_query, db_session)
    if not retailer_voucher_configs:
        raise HttpErrors.INVALID_RETAILER.value

    voucher_config = next(
        (voucher for voucher in retailer_voucher_configs if voucher.voucher_type_slug == voucher_type_slug), None
    )
    if voucher_config is None:
        raise HttpErrors.UNKNOWN_VOUCHER_TYPE.value

    return voucher_config


async def get_allocable_voucher(db_session: AsyncSession, voucher_config: VoucherConfig) -> Optional[Voucher]:
    async def _query() -> Optional[Voucher]:
        return (
            (
                await db_session.execute(
                    select(Voucher).with_for_update().filter_by(voucher_config_id=voucher_config.id, allocated=False)
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


# Synchronous functions
def get_voucher(db_session: "Session", **kwargs) -> Optional[Voucher]:
    """Get voucher by kwargs params"""
    voucher = sync_run_query(
        lambda: db_session.query(Voucher).filter_by(**kwargs).first(),
        db_session,
        rollback_on_exc=False,
    )

    return voucher


def get_distinct_voucher_configs(db_session: "Session") -> Optional[List[VoucherConfig]]:
    """Get distinct list of voucher configs, distinct by retailer_slug"""
    voucher_config_rows = sync_run_query(
        lambda: db_session.query(VoucherConfig).distinct(VoucherConfig.retailer_slug),
        db_session,
        rollback_on_exc=False,
    )

    return list(voucher_config_rows)


def mark_voucher_as_deleted(db_session: "Session", voucher_id: int) -> None:
    sync_run_query(
        lambda: db_session.execute(update(Voucher).where(Voucher.id == voucher_id).values(deleted=True)),
        db_session,
        rollback_on_exc=False,
    )
