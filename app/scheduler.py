import logging
import socket
import typing as t

from logging import Logger
from uuid import uuid4

import sentry_sdk

from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.util import undefined
from redis.exceptions import WatchError

from app.core.config import redis, settings
from app.version import __version__

if settings.SENTRY_DSN:  # pragma: no cover
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        environment=settings.SENTRY_ENV,
        release=__version__,
        traces_sample_rate=settings.SENTRY_TRACES_SAMPLE_RATE,
    )


def is_leader(lock_name: str, *, hostname: str = None) -> bool:  # pragma: no cover
    lock_key = f"{settings.REDIS_KEY_PREFIX}:schedule-lock:{lock_name}"
    if hostname is None:
        hostname = f"{socket.gethostname()}-{uuid4()}"
    is_leader = False

    with redis.pipeline() as pipe:
        try:
            pipe.watch(lock_key)
            leader_host = pipe.get(lock_key)
            if leader_host in (hostname, None):
                pipe.multi()
                pipe.setex(lock_key, 30, hostname)
                pipe.execute()
                is_leader = True
        except WatchError:
            pass  # somebody else changed the key

    return is_leader


class CronScheduler:  # pragma: no cover
    default_schedule = "* * * * *"

    def __init__(
        self,
        *,
        name: str,
        schedule_fn: t.Callable,
        callback: t.Callable,
        coalesce_jobs: t.Optional[bool] = None,
        logger: Logger = None,
    ):
        self.name = name
        self.schedule_fn = schedule_fn
        self.callback = callback
        self.coalesce_jobs = coalesce_jobs if coalesce_jobs is not None else undefined
        self.log = logger if logger is not None else logging.getLogger("cron-scheduler")

    def __str__(self) -> str:
        return f"{self.__class__.__name__} with schedule '{self.schedule_fn()}'"

    def _get_trigger(self, schedule: t.Callable) -> CronTrigger:
        try:
            return CronTrigger.from_crontab(schedule)
        except ValueError:
            self.log.error(
                (
                    f"Schedule '{schedule}' is not in a recognised format! "
                    f"Reverting to default of '{self.default_schedule}'."
                )
            )
            return CronTrigger.from_crontab(self.default_schedule)

    def run(self) -> None:
        scheduler = BlockingScheduler()
        schedule = self.schedule_fn()
        if not schedule:
            self.log.warning((f"No schedule provided! Reverting to default of '{self.default_schedule}'."))
            schedule = self.default_schedule

        scheduler.add_job(self.tick, trigger=self._get_trigger(schedule), coalesce=self.coalesce_jobs)
        scheduler.start()

    def tick(self) -> None:
        try:
            if is_leader(self.name):
                self.callback()
        except Exception as e:
            if settings.SENTRY_DSN:
                self.log.exception(repr(e))
                sentry_sdk.capture_exception()
            else:
                raise
