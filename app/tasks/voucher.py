from app.core.config import settings
from app.retry_task_utils.asynchronous import enqueue_retry_task


async def enqueue_voucher_allocation_retry_task(retry_task_id: int) -> None:
    from app.tasks.allocation import allocate_voucher

    await enqueue_retry_task(
        retry_task_id=retry_task_id, action=allocate_voucher, queue=settings.VOUCHER_ALLOCATION_TASK_QUEUE
    )
