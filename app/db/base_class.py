# mypy checks for sqlalchemy core 2.0 require sqlalchemy2-stubs
from sqlalchemy import Column, DateTime, Integer, text
from sqlalchemy.orm import declarative_base  # type: ignore
from sqlalchemy.orm import declarative_mixin  # type: ignore


class ModelBase:
    id = Column(Integer, primary_key=True, index=True)


Base = declarative_base(cls=ModelBase)

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
