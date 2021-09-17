import rq
import sentry_sdk

from sqlalchemy.future import select

from app.core.config import redis, settings
from app.db.base_class import async_run_query
from app.db.session import AsyncSessionMaker
from app.enums import QueuedRetryStatuses
from app.models import VoucherAllocation, VoucherConfig


async def enqueue_voucher_allocation(voucher_allocation_id: int, voucher_config: VoucherConfig) -> None:
    from app.tasks.allocation import allocate_voucher

    async with AsyncSessionMaker() as db_session:

        async def _get_allocation() -> VoucherAllocation:
            return (
                (
                    await db_session.execute(
                        select(VoucherAllocation)
                        .with_for_update()
                        .filter_by(id=voucher_allocation_id, status=QueuedRetryStatuses.PENDING)
                    )
                )
                .scalars()
                .first()
            )

        async def _update_status_and_flush() -> None:
            voucher_allocation.status = QueuedRetryStatuses.IN_PROGRESS
            await db_session.flush()

        async def _commit() -> None:
            await db_session.commit()

        async def _rollback() -> None:
            await db_session.rollback()

        try:
            q = rq.Queue(settings.VOUCHER_ALLOCATION_TASK_QUEUE, connection=redis)
            voucher_allocation = await async_run_query(_get_allocation, db_session, rollback_on_exc=False)
            await async_run_query(_update_status_and_flush, db_session)
            q.enqueue(
                allocate_voucher,
                voucher_allocation_id=voucher_allocation_id,
                voucher_config=voucher_config,
                failure_ttl=60 * 60 * 24 * 7,  # 1 week
            )
            await async_run_query(_commit, db_session, rollback_on_exc=False)
        except Exception as ex:
            sentry_sdk.capture_exception(ex)
            await async_run_query(_rollback, db_session, rollback_on_exc=False)
