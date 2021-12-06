from retry_tasks_lib.utils.asynchronous import enqueue_many_retry_tasks, enqueue_retry_task

from app.core.config import redis
from app.db.session import AsyncSessionMaker


async def enqueue_task(retry_task_id: int) -> None:  # pragma: no cover
    async with AsyncSessionMaker() as db_session:
        await enqueue_retry_task(
            db_session=db_session,
            retry_task_id=retry_task_id,
            connection=redis,
        )


async def enqueue_many_tasks(retry_tasks_ids: list[int]) -> None:  # pragma: no cover
    async with AsyncSessionMaker() as db_session:
        await enqueue_many_retry_tasks(
            db_session=db_session,
            retry_tasks_ids=retry_tasks_ids,
            connection=redis,
        )
