from typing import Callable

import rq
import sentry_sdk

from sqlalchemy.future import select

from app.core.config import redis, settings
from app.db.base_class import async_run_query
from app.db.session import AsyncSessionMaker
from app.enums import QueuedRetryStatuses
from app.models import RetryTask


async def enqueue_retry_task(retry_task_id: int, action: Callable, queue: str) -> None:

    async with AsyncSessionMaker() as db_session:

        async def _get_retry_task() -> RetryTask:
            return (
                (
                    await db_session.execute(
                        select(RetryTask)
                        .with_for_update()
                        .where(
                            RetryTask.retry_task_id == retry_task_id,
                            RetryTask.retry_status == QueuedRetryStatuses.PENDING,
                        )
                    )
                )
                .scalars()
                .first()
            )

        async def _update_status_and_flush() -> None:
            retry_task.retry_status = QueuedRetryStatuses.IN_PROGRESS
            await db_session.flush()

        async def _commit() -> None:
            await db_session.commit()

        async def _rollback() -> None:
            await db_session.rollback()

        try:
            q = rq.Queue(queue, connection=redis)
            retry_task = await async_run_query(_get_retry_task, db_session, rollback_on_exc=False)
            await async_run_query(_update_status_and_flush, db_session)
            q.enqueue(
                action,
                retry_task_id=retry_task_id,
                failure_ttl=60 * 60 * 24 * 7,  # 1 week
            )
            await async_run_query(_commit, db_session, rollback_on_exc=False)
        except Exception as ex:
            sentry_sdk.capture_exception(ex)
            await async_run_query(_rollback, db_session, rollback_on_exc=False)
