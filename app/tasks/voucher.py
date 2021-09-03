from typing import List

import rq
import sentry_sdk

from sqlalchemy.future import select

from app.core.config import redis, settings
from app.db.base_class import async_run_query
from app.db.session import AsyncSessionMaker
from app.enums import QueuedRetryStatuses
from app.models import VoucherAllocation, VoucherUpdate


async def enqueue_voucher_allocation(voucher_allocation_id: int) -> None:
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

            if voucher_allocation.voucher_id is None:
                # placeholder "no more allocable vouchers" logic

                async def _set_failed() -> None:
                    voucher_allocation.status = QueuedRetryStatuses.FAILED
                    await db_session.commit()

                await async_run_query(_set_failed, db_session)
                return

            await async_run_query(_update_status_and_flush, db_session)
            q.enqueue(
                allocate_voucher,
                voucher_allocation_id=voucher_allocation_id,
                failure_ttl=60 * 60 * 24 * 7,  # 1 week
            )
            await async_run_query(_commit, db_session, rollback_on_exc=False)
        except Exception as ex:
            sentry_sdk.capture_exception(ex)
            await async_run_query(_rollback, db_session, rollback_on_exc=False)


async def enqueue_voucher_status_adjustments(voucher_status_adjustment_ids: List[int]) -> None:
    from app.tasks.status_adjustment import status_adjustment

    async with AsyncSessionMaker() as db_session:

        async def _update_status_and_flush() -> None:
            (
                await db_session.execute(
                    update(VoucherUpdate)  # type: ignore
                    .where(
                        VoucherUpdate.id.in_(voucher_status_adjustment_ids),
                        VoucherUpdate.retry_status == QueuedRetryStatuses.PENDING,
                    )
                    .values(status=QueuedRetryStatuses.IN_PROGRESS)
                )
            )

            await db_session.flush()

        async def _commit() -> None:
            await db_session.commit()

        async def _rollback() -> None:
            await db_session.rollback()

        try:
            q = rq.Queue(settings.VOUCHER_STATUS_UPDATE_TASK_QUEUE, connection=redis)
            await async_run_query(_update_status_and_flush, db_session)
            try:
                q.enqueue_many(
                    [
                        rq.Queue.prepare_data(
                            status_adjustment,
                            kwargs={"voucher_status_adjustment_id": voucher_status_adjustment_id},
                            failure_ttl=60 * 60 * 24 * 7,  # 1 week
                        )
                        for voucher_status_adjustment_id in voucher_status_adjustment_ids
                    ]
                )
            except Exception:
                await async_run_query(_rollback, db_session)
                raise
            else:
                await async_run_query(_commit, db_session, rollback_on_exc=False)

        except Exception as ex:  # pragma: no cover
            sentry_sdk.capture_exception(ex)
