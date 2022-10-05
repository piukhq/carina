from typing import TYPE_CHECKING

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.utils.synchronous import cleanup_handler

from carina.db.session import SyncSessionMaker

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@cleanup_handler(db_session_factory=SyncSessionMaker)
def reward_issuance_cleanup_handler(
    retry_task: RetryTask, db_session: "Session"  # pylint: disable=unused-argument
) -> None:
    retry_task.get_params()
    pass  # pylint: disable=unnecessary-pass
