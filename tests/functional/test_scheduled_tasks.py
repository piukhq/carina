from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from pytest_mock import MockerFixture
from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.enums import RetryTaskStatuses

from carina.scheduled_tasks.task_cleanup import cleanup_old_tasks

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def test_cleanup_old_tasks(
    create_mock_task: "Callable[..., RetryTask]",
    reward_status_adjustment_task_type: "TaskType",
    db_session: "Session",
    mocker: MockerFixture,
) -> None:

    mock_logger = mocker.patch("carina.scheduled_tasks.task_cleanup.logger")

    now = datetime.now(tz=timezone.utc)

    deleteable_task = create_mock_task(reward_status_adjustment_task_type, {"status": RetryTaskStatuses.SUCCESS})
    deleteable_task.created_at = now - timedelta(days=181)
    deleteable_task_id = deleteable_task.retry_task_id

    wrong_status_task = create_mock_task(reward_status_adjustment_task_type, {"status": RetryTaskStatuses.FAILED})
    wrong_status_task.created_at = now - timedelta(days=200)

    not_old_enough_task = create_mock_task(reward_status_adjustment_task_type, {"status": RetryTaskStatuses.SUCCESS})
    not_old_enough_task.created_at = now - timedelta(days=10)

    db_session.commit()

    tz = ZoneInfo("Europe/London")

    mock_time_reference = datetime.now(tz=tz).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=180)

    cleanup_old_tasks()

    db_session.expire_all()

    logger_calls = mock_logger.info.call_args_list

    assert logger_calls[0].args == ("Cleaning up tasks created before %s...", mock_time_reference.date())
    assert logger_calls[1].args == ("Deleted %d tasks. ( °╭ ︿ ╮°)", 1)

    assert not db_session.get(RetryTask, deleteable_task_id)
    assert wrong_status_task.retry_task_id
    assert not_old_enough_task.retry_task_id
