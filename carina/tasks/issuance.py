from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import sentry_sdk

from fastapi import status
from requests.exceptions import HTTPError
from retry_tasks_lib.db.models import RetryTask, TaskTypeKey, TaskTypeKeyValue
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import enqueue_retry_task_delay, retryable_task
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload

from carina.activity_utils.enums import ActivityType
from carina.activity_utils.tasks import sync_send_activity
from carina.core.config import redis_raw, settings
from carina.db.base_class import sync_run_query
from carina.db.session import SyncSessionMaker
from carina.enums import RewardTypeStatuses
from carina.fetch_reward import get_allocable_reward, get_associated_url
from carina.models import Reward, RewardConfig

from . import logger, send_request_with_metrics
from .prometheus import task_processing_time_callback_fn, tasks_run_total

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session


REWARD_ID = "reward_uuid"
CODE = "code"
ISSUED = "issued_date"
EXPIRY = "expiry_date"


def _process_issuance(task_params: dict, validity_days: int | None = None) -> dict:
    logger.info(f"Processing allocation for reward: {task_params['reward_uuid']}")
    response_audit: dict = {"timestamp": datetime.now(tz=timezone.utc).isoformat()}
    parsed_url = urlparse(task_params["account_url"])

    url_template = "{scheme}://{netloc}{path}"
    url_kwargs = {
        "scheme": parsed_url.scheme,
        "netloc": parsed_url.netloc,
        "path": parsed_url.path,
    }
    exclude = ["path"]
    if parsed_url.query:
        url_template += "?{query}"
        url_kwargs["query"] = parsed_url.query
        exclude.append("query")

    # Set issued date and expiry date for pre-loaded rewards else get them from task_params
    now = datetime.now(tz=timezone.utc)
    issued_date: float = task_params.get("issued_date", now.timestamp())
    expiry_date: float | None = task_params.get("expiry_date", None)
    if not expiry_date:
        if not validity_days:
            raise ValueError("Both validity_days and expiry_date are None")

        expiry_date = (now + timedelta(days=validity_days)).timestamp()

    resp = send_request_with_metrics(
        "POST",
        url_template=url_template,
        url_kwargs=url_kwargs,
        exclude_from_label_url=exclude,
        json={
            "code": task_params["code"],
            "issued_date": issued_date,
            "expiry_date": expiry_date,
            "reward_slug": task_params["reward_slug"],
            "reward_uuid": task_params["reward_uuid"],
            "associated_url": get_associated_url(task_params),
        },
        headers={
            "Authorization": f"Token {settings.POLARIS_API_AUTH_TOKEN}",
            "Idempotency-Token": task_params["idempotency_token"],
        },
    )
    resp.raise_for_status()
    response_audit["response"] = {"status": resp.status_code, "body": resp.text}
    logger.info(f"Allocation succeeded for reward: {task_params['reward_uuid']}")

    sync_send_activity(
        ActivityType.get_reward_status_activity_data(
            account_url_path=parsed_url.path,
            retailer_slug=task_params["retailer_slug"],
            reward_slug=task_params["reward_slug"],
            activity_timestamp=issued_date,
            reward_uuid=task_params["reward_uuid"],
            pending_reward_id=task_params.get("pending_reward_id", None),
        ),
        routing_key=ActivityType.REWARD_STATUS.value,
    )

    return response_audit


def _get_reward_config(db_session: "Session", reward_config_id: int) -> RewardConfig:

    return sync_run_query(
        lambda: db_session.execute(
            select(RewardConfig).options(joinedload(RewardConfig.retailer)).where(RewardConfig.id == reward_config_id)
        ).scalar_one(),
        db_session,
    )


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

    def _query() -> None:
        db_session.execute(Reward.__table__.update().values(allocated=True).where(Reward.id == reward_uuid))
        db_session.execute(
            TaskTypeKeyValue.__table__.delete().where(
                TaskTypeKeyValue.retry_task_id == retry_task.retry_task_id,
                TaskTypeKeyValue.task_type_key_id == TaskTypeKey.task_type_key_id,
                TaskTypeKey.name.in_(["reward_uuid", "code", "issued_date", "expiry_date"]),
            )
        )
        db_session.commit()

    sync_run_query(_query, db_session)


def _process_and_issue_reward(db_session: "Session", retry_task: RetryTask, validity_days: int | None = None) -> None:
    task_params = retry_task.get_params()
    try:
        response_audit = _process_issuance(task_params, validity_days)
    except HTTPError as ex:
        if ex.response.status_code == status.HTTP_409_CONFLICT:
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
@retryable_task(db_session_factory=SyncSessionMaker, metrics_callback_fn=task_processing_time_callback_fn)
def issue_reward(retry_task: RetryTask, db_session: "Session") -> None:
    """Try to fetch and issue a reward, unless the campaign has been cancelled"""
    if settings.ACTIVATE_TASKS_METRICS:
        tasks_run_total.labels(app=settings.PROJECT_NAME, task_name=settings.REWARD_ISSUANCE_TASK_NAME).inc()

    reward_config = _get_reward_config(db_session, retry_task.get_params()["reward_config_id"])
    if reward_config.status == RewardTypeStatuses.CANCELLED:
        _cancel_task(db_session, retry_task)
        return

    # Process the allocation if it has a reward, else try to get a reward - requeue that if necessary
    if "reward_uuid" in retry_task.get_params():
        validity_days = reward_config.load_required_fields_values().get("validity_days")
        _process_and_issue_reward(db_session, retry_task, validity_days)
    else:
        reward_data = get_allocable_reward(db_session, reward_config, retry_task)

        if reward_data.reward is not None:
            key_ids = retry_task.task_type.get_key_ids_by_name()

            def _add_reward_to_task_values_and_set_allocated(reward: Reward) -> None:
                reward.allocated = True
                key_ids_to_add = [
                    (key_ids[REWARD_ID], str(reward.id)),
                    (key_ids[CODE], reward.code),
                ]

                # If expiry_date is available e.g. jigsaw or pre_loaded with fixed expiry,
                # add it to task_type_key_values
                if reward_data.expiry_date:
                    key_ids_to_add.append((key_ids[EXPIRY], reward_data.expiry_date))

                # If issued_date is available e.g. jigsaw, add it to task_type_key_values
                if reward_data.issued_date:
                    key_ids_to_add.append((key_ids[ISSUED], reward_data.issued_date))

                db_session.add_all(retry_task.get_task_type_key_values(key_ids_to_add))
                db_session.commit()

            sync_run_query(_add_reward_to_task_values_and_set_allocated, db_session, reward=reward_data.reward)
            db_session.refresh(retry_task)  # Ensure retry_task represents latest DB changes
            _process_and_issue_reward(db_session, retry_task, reward_data.validity_days)
        else:  # requeue the allocation attempt and alert if required
            if settings.MESSAGE_IF_NO_PRE_LOADED_REWARDS:
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
                retry_task.status = RetryTaskStatuses.WAITING
                db_session.commit()

            sync_run_query(_set_waiting, db_session)

            next_attempt_time = enqueue_retry_task_delay(
                connection=redis_raw,
                retry_task=retry_task,
                delay_seconds=settings.REWARD_ISSUANCE_REQUEUE_BACKOFF_SECONDS,
            )
            logger.info(f"Next attempt time at {next_attempt_time}")
            retry_task.update_task(db_session, next_attempt_time=next_attempt_time)
