from datetime import datetime

from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import get_retry_task

from app.core.config import settings
from app.db.session import SyncSessionMaker

from . import logger, send_request_with_metrics
from .prometheus import tasks_run_total


def _process_status_adjustment(task_params: dict) -> dict:
    logger.info(f"Processing status adjustment for voucher: {task_params['voucher_id']}")
    timestamp = datetime.utcnow()
    response_audit: dict = {"timestamp": timestamp.isoformat()}

    resp = send_request_with_metrics(
        "PATCH",
        "{base_url}/bpl/loyalty/{retailer_slug}/rewards/{voucher_id}/status".format(
            base_url=settings.POLARIS_URL,
            retailer_slug=task_params["retailer_slug"],
            voucher_id=task_params["voucher_id"],
        ),
        json={
            "status": task_params["status"],
            "date": task_params["date"],
        },
        headers={"Authorization": f"Token {settings.POLARIS_AUTH_TOKEN}"},
        timeout=(3.03, 10),
    )
    resp.raise_for_status()
    response_audit["response"] = {"status": resp.status_code, "body": resp.text}
    logger.info(f"Status adjustment succeeded for voucher: {task_params['voucher_id']}")

    return response_audit


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
def status_adjustment(retry_task_id: int) -> None:
    tasks_run_total.labels(app=settings.PROJECT_NAME, task_name=settings.VOUCHER_STATUS_ADJUSTMENT_TASK_NAME).inc()
    with SyncSessionMaker() as db_session:

        retry_task = get_retry_task(db_session, retry_task_id)
        retry_task.update_task(db_session, increase_attempts=True)

        response_audit = _process_status_adjustment(retry_task.get_params())

        retry_task.update_task(
            db_session, response_audit=response_audit, status=RetryTaskStatuses.SUCCESS, clear_next_attempt_time=True
        )
