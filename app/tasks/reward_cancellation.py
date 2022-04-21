from datetime import datetime, timezone
from typing import TYPE_CHECKING

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import retryable_task

from app.core.config import settings
from app.db.session import SyncSessionMaker

from . import logger, send_request_with_metrics
from .prometheus import tasks_run_total

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session


def _process_rewards_cancellation(task_params: dict) -> dict:
    logger.info(f"Processing rewards' cancellation for reward type: {task_params['reward_slug']}")
    response_audit: dict = {"timestamp": datetime.now(tz=timezone.utc).isoformat()}

    resp = send_request_with_metrics(
        "POST",
        url_template="{base_url}/{retailer_slug}/rewards/{reward_slug}/cancel",
        url_kwargs={
            "base_url": settings.POLARIS_BASE_URL,
            "retailer_slug": task_params["retailer_slug"],
            "reward_slug": task_params["reward_slug"],
        },
        exclude_from_label_url=["reward_slug"],
        headers={"Authorization": f"Token {settings.POLARIS_API_AUTH_TOKEN}"},
    )
    resp.raise_for_status()
    response_audit["response"] = {"status": resp.status_code, "body": resp.text}
    logger.info(f"Rewards' cancellation succeeded for reward type: {task_params['reward_slug']}")

    return response_audit


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
@retryable_task(db_session_factory=SyncSessionMaker)
def cancel_rewards(retry_task: RetryTask, db_session: "Session") -> None:
    tasks_run_total.labels(app=settings.PROJECT_NAME, task_name=settings.CANCEL_REWARDS_TASK_NAME).inc()
    response_audit = _process_rewards_cancellation(retry_task.get_params())
    retry_task.update_task(
        db_session, response_audit=response_audit, status=RetryTaskStatuses.SUCCESS, clear_next_attempt_time=True
    )
