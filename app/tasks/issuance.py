from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

import sentry_sdk

from fastapi import status
from requests.exceptions import HTTPError
from retry_tasks_lib.db.models import RetryTask, TaskTypeKeyValue
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import enqueue_retry_task_delay, retryable_task
from sqlalchemy.future import select

from app.core.config import redis, settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.enums import RewardTypeStatuses
from app.models import Reward, RewardConfig

from . import logger, send_request_with_metrics

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session


REWARD_ID = "reward_uuid"
CODE = "code"


def _process_issuance(task_params: dict) -> dict:
    logger.info(f"Processing allocation for reward: {task_params['reward_uuid']}")
    response_audit: dict = {"timestamp": datetime.now(tz=timezone.utc).isoformat()}

    resp = send_request_with_metrics(
        "POST",
        task_params["account_url"],
        json={
            "code": task_params["code"],
            "issued_date": task_params["issued_date"],
            "expiry_date": task_params["expiry_date"],
            "reward_slug": task_params["reward_slug"],
            "reward_uuid": task_params["reward_uuid"],
        },
        headers={
            "Authorization": f"Token {settings.POLARIS_AUTH_TOKEN}",
            "Idempotency-Token": task_params["idempotency_token"],
        },
        timeout=(3.03, 10),
    )
    resp.raise_for_status()
    response_audit["response"] = {"status": resp.status_code, "body": resp.text}
    logger.info(f"Allocation succeeded for reward: {task_params['reward_uuid']}")

    return response_audit


def _get_reward_config_status(db_session: "Session", reward_config_id: int) -> RewardTypeStatuses:
    reward_config_status: RewardTypeStatuses = sync_run_query(
        lambda: db_session.execute(select(RewardConfig.status).where(RewardConfig.id == reward_config_id)).scalar_one(),
        db_session,
    )

    return reward_config_status


def _get_reward(db_session: "Session", reward_uuid: str) -> Reward:
    reward: Reward = sync_run_query(
        lambda: db_session.execute(select(Reward).where(Reward.id == reward_uuid)).scalar_one(),
        db_session,
    )

    return reward


def _cancel_task(db_session: "Session", retry_task: RetryTask) -> None:
    """The campaign been cancelled: cancel the task and soft delete any associated reward"""
    retry_task.update_task(db_session, status=RetryTaskStatuses.CANCELLED, clear_next_attempt_time=True)
    task_params = retry_task.get_params()

    if task_params.get("reward_uuid"):
        reward: Reward = _get_reward(db_session, task_params.get("reward_uuid"))
        reward.deleted = True
        db_session.commit()


def _set_reward_and_delete_from_task(db_session: "Session", retry_task: RetryTask, reward_uuid: str) -> None:
    """
    set reward allocated and clear the retry task's reward id to force a complete retry
    of the task with a new reward
    """
    reward: Reward = _get_reward(db_session, reward_uuid)
    reward.allocated = True
    # Now delete the associated reward id and code in the DB
    values_to_delete: dict[str, TaskTypeKeyValue] = {
        value.task_type_key.name: value
        for value in retry_task.task_type_key_values
        if value.task_type_key.name in ("reward_uuid", "code")
    }
    db_session.delete(values_to_delete["reward_uuid"])
    db_session.delete(values_to_delete["code"])
    db_session.commit()


def _process_and_issue_reward(db_session: "Session", retry_task: RetryTask) -> None:
    task_params = retry_task.get_params()
    try:
        response_audit = _process_issuance(task_params)
    except HTTPError as e:
        if e.response.status_code == status.HTTP_409_CONFLICT:
            _set_reward_and_delete_from_task(
                db_session=db_session, retry_task=retry_task, reward_uuid=task_params.get("reward_uuid")
            )
        raise
    else:
        retry_task.update_task(
            db_session, response_audit=response_audit, status=RetryTaskStatuses.SUCCESS, clear_next_attempt_time=True
        )


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
@retryable_task(db_session_factory=SyncSessionMaker)
def issue_reward(retry_task: RetryTask, db_session: "Session") -> None:
    """Try to fetch and issue a reward, unless the campaign has been cancelled"""

    print(retry_task.get_params())
    reward_config_status = _get_reward_config_status(db_session, retry_task.get_params()["reward_config_id"])
    if reward_config_status == RewardTypeStatuses.CANCELLED:
        _cancel_task(db_session, retry_task)
        return

    # Process the allocation if it has a reward, else try to get a reward - requeue that if necessary
    if "reward_uuid" in retry_task.get_params():
        _process_and_issue_reward(db_session, retry_task)
    else:

        def _get_allocable_reward() -> Optional[Reward]:
            allocable_reward = (
                db_session.execute(
                    select(Reward)
                    .with_for_update()
                    .where(
                        Reward.reward_config_id == retry_task.get_params()["reward_config_id"],
                        Reward.allocated == False,  # noqa
                        Reward.deleted == False,  # noqa
                    )
                    .limit(1)
                )
                .scalars()
                .first()
            )

            return allocable_reward

        allocable_reward: Reward = sync_run_query(_get_allocable_reward, db_session)
        if allocable_reward:
            key_ids = retry_task.task_type.get_key_ids_by_name()

            def _add_reward_to_task_values_and_set_allocated() -> None:
                allocable_reward.allocated = True
                db_session.add_all(
                    retry_task.get_task_type_key_values(
                        [
                            (key_ids[REWARD_ID], str(allocable_reward.id)),
                            (key_ids[CODE], allocable_reward.code),
                        ]
                    )
                )

                db_session.commit()

            sync_run_query(_add_reward_to_task_values_and_set_allocated, db_session)
            db_session.refresh(retry_task)  # Ensure retry_task represents latest DB changes
            _process_and_issue_reward(db_session, retry_task)
        else:  # requeue the allocation attempt
            if retry_task.status != RetryTaskStatuses.WAITING:
                # Only do a Sentry alert for the first allocation failure (when status is changing to WAITING)
                with sentry_sdk.push_scope() as scope:
                    scope.fingerprint = ["{{ default }}", "{{ message }}"]
                    event_id = sentry_sdk.capture_message(
                        f"No Reward Codes Available for RewardConfig: "
                        f"{retry_task.get_params()['reward_config_id']}, "
                        f"reward slug: {retry_task.get_params()['reward_slug']} "
                        f"on {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')}"
                    )
                    logger.info(f"Sentry event ID: {event_id}")

                def _set_waiting() -> None:
                    retry_task.status = RetryTaskStatuses.WAITING.name
                    db_session.commit()

                sync_run_query(_set_waiting, db_session)

            next_attempt_time = enqueue_retry_task_delay(
                connection=redis,
                retry_task=retry_task,
                delay_seconds=settings.REWARD_ISSUANCE_REQUEUE_BACKOFF_SECONDS,
            )
            logger.info(f"Next attempt time at {next_attempt_time}")
            retry_task.update_task(db_session, next_attempt_time=next_attempt_time)
