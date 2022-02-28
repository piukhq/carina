import logging
import socket

from functools import wraps
from logging import Logger
from typing import Any, Callable, Optional, Protocol
from uuid import uuid4

from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.util import undefined
from redis.exceptions import WatchError

from app.core.config import redis, settings
from app.scheduled_tasks import logger as scheduled_tasks_logger

from . import logger


class Runner(Protocol):
    id: str
    name: str


def run_only_if_leader(runner: Runner) -> Callable:
    """
    Decorator for use with scheduled tasks to determine whether the host/pod is
    the leader and thus whether to execute the task or not.

    """

    def decorater(func: Callable) -> Callable:
        host_leader_id = f"{socket.gethostname()}-{runner.id}"

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            func_lock_key = f"{settings.REDIS_KEY_PREFIX}{runner.name}:{func.__qualname__}"
            with redis.pipeline() as pipe:
                try:
                    pipe.watch(func_lock_key)
                    cached_leader_id = pipe.get(func_lock_key)
                    if cached_leader_id in (host_leader_id, None):
                        pipe.multi()
                        pipe.setex(func_lock_key, 30, host_leader_id)
                        pipe.execute()
                        return func(*args, **kwargs)
                    else:
                        logger.info(
                            f"Leader with id {host_leader_id} could not run {func.__qualname__}. Not the leader."
                        )
                except WatchError:
                    # somebody else changed the key
                    logger.info(
                        f"Leader with id {host_leader_id} could not run {func.__qualname__}. "
                        "Could not acquire leader lock."
                    )

        return wrapper

    return decorater


class CronScheduler:  # pragma: no cover
    name = "cron-scheduler"
    default_schedule = "* * * * *"

    def __init__(self, *, logger: Logger = None):
        self.id = str(uuid4())
        self.log = logger if logger is not None else logging.getLogger("cron-scheduler")
        self._scheduler = BlockingScheduler()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(id: {self.id})"

    def _get_trigger(self, schedule: Callable) -> CronTrigger:
        tz = "Europe/London"
        try:
            return CronTrigger.from_crontab(schedule, timezone=tz)
        except ValueError:
            self.log.error(
                (
                    f"Schedule '{schedule}' is not in a recognised format! "
                    f"Reverting to default of '{self.default_schedule}'."
                )
            )
            return CronTrigger.from_crontab(self.default_schedule, timezone=tz)

    def add_job(
        self,
        job_func: Callable,
        schedule_fn: Callable,
        coalesce_jobs: Optional[bool] = None,
    ) -> None:
        if coalesce_jobs is None:
            coalesce_jobs = undefined
        schedule = schedule_fn()
        if not schedule:
            self.log.warning((f"No schedule provided! Reverting to default of '{self.default_schedule}'."))
            schedule = self.default_schedule
        self._scheduler.add_job(job_func, trigger=self._get_trigger(schedule), coalesce=coalesce_jobs)

    def run(self) -> None:
        self._scheduler.start()


cron_scheduler = CronScheduler(logger=scheduled_tasks_logger)
