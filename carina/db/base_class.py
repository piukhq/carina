# mypy checks for sqlalchemy core 2.0 require sqlalchemy2-stubs
import logging

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import sentry_sdk

from retry_tasks_lib.db.models import load_models_to_metadata
from sqlalchemy import Column, DateTime, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import declarative_base, declarative_mixin

from carina.core.config import settings

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session, SessionTransaction

logger = logging.getLogger("db-base-class")


Base = declarative_base()
load_models_to_metadata(Base.metadata)


utc_timestamp_sql = text("TIMEZONE('utc', CURRENT_TIMESTAMP)")


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
    use_savepoint: bool = False,
    **kwargs: Any,
) -> Any:  # pragma: no cover
    savepoint: "SessionTransaction | None" = None
    while attempts > 0:
        attempts -= 1
        try:
            if use_savepoint:
                savepoint = session.begin_nested()
                kwargs["db_savepoint"] = savepoint

            return fn(**kwargs)
        except DBAPIError as ex:
            logger.debug(f"Attempt failed: {type(ex).__name__} {ex}")
            if rollback_on_exc:
                if savepoint:
                    savepoint.rollback()
                else:
                    session.rollback()

            if attempts > 0 and ex.connection_invalidated:
                logger.warning(f"Interrupted transaction: {ex!r}, attempts remaining:{attempts}")
            else:
                sentry_sdk.capture_message(f"Max db connection attempts reached: {ex!r}")
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
                logger.warning(f"Interrupted transaction: {ex!r}, attempts remaining:{attempts}")
            else:
                sentry_sdk.capture_message(f"Max db connection attempts reached: {ex!r}")
                raise
