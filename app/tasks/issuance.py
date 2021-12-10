from datetime import datetime
from typing import TYPE_CHECKING, Optional

import sentry_sdk

from requests.exceptions import HTTPError
from retry_tasks_lib.db.models import RetryTask, TaskTypeKeyValue
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import enqueue_retry_task_delay, get_retry_task
from sqlalchemy.future import select
from starlette.status import HTTP_409_CONFLICT

from app.core.config import redis, settings
from app.db.base_class import sync_run_query
from app.db.session import SyncSessionMaker
from app.enums import VoucherTypeStatuses
from app.models import Voucher, VoucherConfig

from . import logger, send_request_with_metrics

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session


VOUCHER_ID = "voucher_id"
VOUCHER_CODE = "voucher_code"


def _process_issuance(task_params: dict) -> dict:
    logger.info(f"Processing allocation for voucher: {task_params['voucher_id']}")
    timestamp = datetime.utcnow()
    response_audit: dict = {"timestamp": timestamp.isoformat()}

    resp = send_request_with_metrics(
        "POST",
        task_params["account_url"],
        json={
            "voucher_code": task_params["voucher_code"],
            "issued_date": task_params["issued_date"],
            "expiry_date": task_params["expiry_date"],
            "voucher_type_slug": task_params["voucher_type_slug"],
            "voucher_id": task_params["voucher_id"],
        },
        headers={
            "Authorization": f"Token {settings.POLARIS_AUTH_TOKEN}",
            "Idempotency-Token": task_params["idempotency_token"],
        },
        timeout=(3.03, 10),
    )
    resp.raise_for_status()
    response_audit["response"] = {"status": resp.status_code, "body": resp.text}
    logger.info(f"Allocation succeeded for voucher: {task_params['voucher_id']}")

    return response_audit


def _get_voucher_config_status(db_session: "Session", voucher_config_id: int) -> VoucherTypeStatuses:
    voucher_config_status: VoucherTypeStatuses = sync_run_query(
        lambda: db_session.execute(
            select(VoucherConfig.status).where(VoucherConfig.id == voucher_config_id)
        ).scalar_one(),
        db_session,
    )

    return voucher_config_status


def _get_voucher(db_session: "Session", voucher_id: str) -> Voucher:
    voucher: Voucher = sync_run_query(
        lambda: db_session.execute(select(Voucher).where(Voucher.id == voucher_id)).scalar_one(),
        db_session,
    )

    return voucher


def _cancel_task(db_session: "Session", retry_task: RetryTask) -> None:
    """The campaign been cancelled: cancel the task and soft delete any associated voucher"""
    retry_task.update_task(db_session, status=RetryTaskStatuses.CANCELLED, clear_next_attempt_time=True)
    task_params = retry_task.get_params()

    if task_params.get("voucher_id"):
        voucher: Voucher = _get_voucher(db_session, task_params.get("voucher_id"))
        voucher.deleted = True
        db_session.commit()


def _set_voucher_and_delete_from_task(
    db_session: "Session", retry_task: RetryTask, voucher_id: str, allocated: bool = True
) -> None:
    """
    set voucher allocated and clear the retry task's voucher id to force a complete retry
    of the task with a new voucher
    """
    voucher: Voucher = _get_voucher(db_session, voucher_id)
    voucher.allocated = allocated
    # Now delete the associated voucher id and voucher code in the DB
    values_to_delete: dict[str, TaskTypeKeyValue] = {
        value.task_type_key.name: value
        for value in retry_task.task_type_key_values
        if value.task_type_key.name in ("voucher_id", "voucher_code")
    }
    db_session.delete(values_to_delete["voucher_id"])
    db_session.delete(values_to_delete["voucher_code"])
    db_session.commit()


def _process_and_issue_voucher(db_session: "Session", retry_task: RetryTask) -> None:
    retry_task.update_task(db_session, increase_attempts=True)
    task_params = retry_task.get_params()
    try:
        response_audit = _process_issuance(task_params)
    except HTTPError as e:
        if e.response.status_code == HTTP_409_CONFLICT:
            _set_voucher_and_delete_from_task(
                db_session=db_session,
                retry_task=retry_task,
                voucher_id=task_params.get("voucher_id"),
                allocated=True,
            )
        raise
    else:
        retry_task.update_task(
            db_session, response_audit=response_audit, status=RetryTaskStatuses.SUCCESS, clear_next_attempt_time=True
        )


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
def issue_voucher(retry_task_id: int) -> None:
    """Try to fetch and issue a voucher, unless the campaign has been cancelled"""
    with SyncSessionMaker() as db_session:
        retry_task = get_retry_task(db_session, retry_task_id)

        voucher_config_status = _get_voucher_config_status(db_session, retry_task.get_params()["voucher_config_id"])
        if voucher_config_status == VoucherTypeStatuses.CANCELLED:
            _cancel_task(db_session, retry_task)
            return

        # Process the allocation if it has a voucher, else try to get a voucher - requeue that if necessary
        if "voucher_id" in retry_task.get_params():
            _process_and_issue_voucher(db_session, retry_task)
        else:

            def _get_allocable_voucher() -> Optional[Voucher]:
                allocable_voucher = (
                    db_session.execute(
                        select(Voucher)
                        .with_for_update()
                        .where(
                            Voucher.voucher_config_id == retry_task.get_params()["voucher_config_id"],
                            Voucher.allocated == False,  # noqa
                            Voucher.deleted == False,  # noqa
                        )
                        .limit(1)
                    )
                    .scalars()
                    .first()
                )

                return allocable_voucher

            allocable_voucher: Voucher = sync_run_query(_get_allocable_voucher, db_session)
            if allocable_voucher:
                key_ids = retry_task.task_type.get_key_ids_by_name()

                def _add_voucher_to_task_values_and_set_allocated() -> None:
                    allocable_voucher.allocated = True
                    db_session.add_all(
                        retry_task.get_task_type_key_values(
                            [
                                (key_ids[VOUCHER_ID], str(allocable_voucher.id)),
                                (key_ids[VOUCHER_CODE], allocable_voucher.voucher_code),
                            ]
                        )
                    )

                    db_session.commit()

                sync_run_query(_add_voucher_to_task_values_and_set_allocated, db_session)
                db_session.refresh(retry_task)  # Ensure retry_task represents latest DB changes
                _process_and_issue_voucher(db_session, retry_task)
            else:  # requeue the allocation attempt
                if retry_task.status != RetryTaskStatuses.WAITING:
                    # Only do a Sentry alert for the first allocation failure (when status is changing to WAITING)
                    with sentry_sdk.push_scope() as scope:
                        scope.fingerprint = ["{{ default }}", "{{ message }}"]
                        event_id = sentry_sdk.capture_message(
                            f"No Voucher Codes Available for VoucherConfig: "
                            f"{retry_task.get_params()['voucher_config_id']}, "
                            f"voucher type slug: {retry_task.get_params()['voucher_type_slug']} "
                            f"on {datetime.utcnow().strftime('%Y-%m-%d')}"
                        )
                        logger.info(f"Sentry event ID: {event_id}")

                    def _set_waiting() -> None:
                        retry_task.status = RetryTaskStatuses.WAITING.name
                        db_session.commit()

                    sync_run_query(_set_waiting, db_session)

                next_attempt_time = enqueue_retry_task_delay(
                    connection=redis,
                    retry_task=retry_task,
                    delay_seconds=settings.VOUCHER_ISSUANCE_REQUEUE_BACKOFF_SECONDS,
                )
                logger.info(f"Next attempt time at {next_attempt_time}")
                retry_task.update_task(db_session, next_attempt_time=next_attempt_time, increase_attempts=True)
