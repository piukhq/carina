from retry_tasks_lib.utils.asynchronous import enqueue_retry_task

from app.core.config import redis, settings
from app.db.session import AsyncSessionMaker


async def enqueue_voucher_allocation_retry_task(retry_task_id: int) -> None:  # pragma: no cover
    from app.tasks.allocation import issue_voucher

    async with AsyncSessionMaker() as db_session:
        await enqueue_retry_task(
            async_db_session=db_session,
            retry_task_id=retry_task_id,
            action=issue_voucher,
            queue=settings.VOUCHER_ALLOCATION_TASK_QUEUE,
            connection=redis,
        )
