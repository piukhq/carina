from retry_tasks_lib.utils.asynchronous import enqueue_retry_task

from app.core.config import redis
from app.db.session import AsyncSessionMaker


async def enqueue_voucher_allocation_retry_task(retry_task_id: int) -> None:  # pragma: no cover

    async with AsyncSessionMaker() as db_session:
        await enqueue_retry_task(db_session=db_session, retry_task_id=retry_task_id, connection=redis)
