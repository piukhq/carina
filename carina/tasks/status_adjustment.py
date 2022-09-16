from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import UUID

from fastapi import status
from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import retryable_task
from sqlalchemy import update

from carina.core.config import settings
from carina.db.base_class import sync_run_query
from carina.db.session import SyncSessionMaker
from carina.models import Reward

from . import logger, send_request_with_metrics
from .prometheus import task_processing_time_callback_fn, tasks_run_total

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session


def _soft_delete_reward(db_session: "Session", reward_uuid: str) -> None:
    def _query() -> None:
        db_session.execute(
            update(Reward)
            .where(
                Reward.allocated.is_(True),
                Reward.id == UUID(reward_uuid),
            )
            .values(deleted=True)
            .execution_options(synchronize_session=False)
        )
        db_session.commit()

    sync_run_query(_query, db_session)
    logger.info("Soft deleted reward with uuid %s", reward_uuid)


def _process_status_adjustment(db_session: "Session", task_params: dict) -> dict:
    logger.info(f"Processing status adjustment for reward: {task_params['reward_uuid']}")
    response_audit: dict = {"timestamp": datetime.now(tz=timezone.utc).isoformat()}

    resp = send_request_with_metrics(
        "PATCH",
        url_template="{base_url}/{retailer_slug}/rewards/{reward_uuid}/status",
        url_kwargs={
            "base_url": settings.POLARIS_BASE_URL,
            "retailer_slug": task_params["retailer_slug"],
            "reward_uuid": task_params["reward_uuid"],
        },
        exclude_from_label_url=["reward_uuid"],
        json={
            "status": task_params["status"],
            "date": task_params["date"],
        },
        headers={"Authorization": f"Token {settings.POLARIS_API_AUTH_TOKEN}"},
    )
    if resp.status_code == status.HTTP_404_NOT_FOUND:
        _soft_delete_reward(db_session, task_params["reward_uuid"])
    resp.raise_for_status()
    response_audit["response"] = {"status": resp.status_code, "body": resp.text}
    logger.info(f"Status adjustment succeeded for reward: {task_params['reward_uuid']}")

    return response_audit


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
@retryable_task(db_session_factory=SyncSessionMaker, metrics_callback_fn=task_processing_time_callback_fn)
def status_adjustment(retry_task: RetryTask, db_session: "Session") -> None:
    if settings.ACTIVATE_TASKS_METRICS:
        tasks_run_total.labels(app=settings.PROJECT_NAME, task_name=settings.REWARD_STATUS_ADJUSTMENT_TASK_NAME).inc()

    response_audit = _process_status_adjustment(db_session, retry_task.get_params())
    retry_task.update_task(
        db_session, response_audit=response_audit, status=RetryTaskStatuses.SUCCESS, clear_next_attempt_time=True
    )
