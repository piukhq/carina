from datetime import datetime, timezone
from typing import TYPE_CHECKING

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import retryable_task

from app.core.config import settings
from app.db.session import SyncSessionMaker

from . import logger, send_request_with_metrics
from .prometheus import tasks_run_total

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _process_status_adjustment(task_params: dict) -> dict:
    logger.info(f"Processing status adjustment for reward: {task_params['reward_uuid']}")
    response_audit: dict = {"timestamp": datetime.now(tz=timezone.utc).isoformat()}

    resp = send_request_with_metrics(
        "PATCH",
        "{base_url}/bpl/loyalty/{retailer_slug}/rewards/{reward_uuid}/status".format(
            base_url=settings.POLARIS_URL,
            retailer_slug=task_params["retailer_slug"],
            reward_uuid=task_params["reward_uuid"],
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
    logger.info(f"Status adjustment succeeded for reward: {task_params['reward_uuid']}")

    return response_audit


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
@retryable_task(db_session_factory=SyncSessionMaker)
def status_adjustment(retry_task: RetryTask, db_session: "Session") -> None:
    tasks_run_total.labels(app=settings.PROJECT_NAME, task_name=settings.REWARD_STATUS_ADJUSTMENT_TASK_NAME).inc()
    response_audit = _process_status_adjustment(retry_task.get_params())
    retry_task.update_task(
        db_session, response_audit=response_audit, status=RetryTaskStatuses.SUCCESS, clear_next_attempt_time=True
    )
