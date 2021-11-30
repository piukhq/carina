from datetime import datetime

from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import get_retry_task

from app.core.config import settings
from app.db.session import SyncSessionMaker

from . import logger, send_request_with_metrics


def _process_vouchers_cancellation(task_params: dict) -> dict:
    logger.info(f"Processing vouchers' cancellation for voucher type: {task_params['voucher_type_slug']}")
    timestamp = datetime.utcnow()
    response_audit: dict = {"timestamp": timestamp.isoformat()}

    resp = send_request_with_metrics(
        "POST",
        "{base_url}/bpl/loyalty/{retailer_slug}/vouchers/{voucher_type_slug}/cancel".format(
            base_url=settings.POLARIS_URL,
            retailer_slug=task_params["retailer_slug"],
            voucher_type_slug=task_params["voucher_type_slug"],
        ),
        headers={"Authorization": f"Token {settings.POLARIS_AUTH_TOKEN}"},
        timeout=(3.03, 10),
    )
    resp.raise_for_status()
    response_audit["response"] = {"status": resp.status_code, "body": resp.text}
    logger.info(f"Vouchers' cancellation succeeded for voucher type: {task_params['voucher_type_slug']}")

    return response_audit


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
def cancel_vouchers(retry_task_id: int) -> None:
    with SyncSessionMaker() as db_session:

        retry_task = get_retry_task(db_session, retry_task_id)
        retry_task.update_task(db_session, increase_attempts=True)

        response_audit = _process_vouchers_cancellation(retry_task.get_params())

        retry_task.update_task(
            db_session, response_audit=response_audit, status=RetryTaskStatuses.SUCCESS, clear_next_attempt_time=True
        )
