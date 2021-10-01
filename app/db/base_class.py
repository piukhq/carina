# mypy checks for sqlalchemy core 2.0 require sqlalchemy2-stubs
import logging

from typing import TYPE_CHECKING, Any, Callable

import sentry_sdk

from sqlalchemy import Column, DateTime, Integer, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import declarative_base, declarative_mixin  # type: ignore[attr-defined]

from app.core.config import settings
from app.version import __version__

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session

logger = logging.getLogger("db-base-class")


class ModelBase:
    id = Column(Integer, primary_key=True, index=True)


Base = declarative_base(cls=ModelBase)

utc_timestamp_sql = text("TIMEZONE('utc', CURRENT_TIMESTAMP)")

if settings.SENTRY_DSN:
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        environment=settings.SENTRY_ENV,
        traces_sample_rate=settings.SENTRY_TRACES_SAMPLE_RATE,
        release=__version__,
    )


@declarative_mixin
class TimestampMixin:
    created_at = Column(DateTime, server_default=utc_timestamp_sql, nullable=False)
    updated_at = Column(
        DateTime,
        server_default=utc_timestamp_sql,
        onupdate=utc_timestamp_sql,
        nullable=False,
    )


# based on the following stackoverflow answer:
# https://stackoverflow.com/a/30004941
def sync_run_query(
    fn: Callable,
    session: "Session",
    *,
    attempts: int = settings.DB_CONNECTION_RETRY_TIMES,
    rollback_on_exc: bool = True,
    **kwargs: Any,
) -> Any:  # pragma: no cover

    while attempts > 0:
        attempts -= 1
        try:
            return fn(**kwargs)
        except DBAPIError as ex:
            logger.debug(f"Attempt failed: {type(ex).__name__} {ex}")
            if rollback_on_exc:
                session.rollback()

            if attempts > 0 and ex.connection_invalidated:
                logger.warning(f"Interrupted transaction: {repr(ex)}, attempts remaining:{attempts}")
            else:
                sentry_sdk.capture_message(f"Max db connection attempts reached: {repr(ex)}")
                raise


async def async_run_query(
    fn: Callable,
    session: "AsyncSession",
    *,
    attempts: int = settings.DB_CONNECTION_RETRY_TIMES,
    rollback_on_exc: bool = True,
    **kwargs: Any,
) -> Any:  # pragma: no cover
    while attempts > 0:
        attempts -= 1
        try:
            return await fn(**kwargs)
        except DBAPIError as ex:
            logger.debug(f"Attempt failed: {type(ex).__name__} {ex}")
            if rollback_on_exc:
                await session.rollback()

            if attempts > 0 and ex.connection_invalidated:
                logger.warning(f"Interrupted transaction: {repr(ex)}, attempts remaining:{attempts}")
            else:
                sentry_sdk.capture_message(f"Max db connection attempts reached: {repr(ex)}")
                raise
