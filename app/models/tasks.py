from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import String

from app.db.base_class import Base, TimestampMixin
from app.enums import QueuedRetryStatuses, TaskParamsKeyTypes


class RetryTask(Base, TimestampMixin):
    __tablename__ = "retry_task"

    retry_task_id = Column(Integer, primary_key=True)
    attempts = Column(Integer, default=0, nullable=False)
    response_data = Column(MutableList.as_mutable(JSONB), nullable=False, default=text("'[]'::jsonb"))
    next_attempt_time = Column(DateTime, nullable=True)
    retry_status = Column(Enum(QueuedRetryStatuses), nullable=False, default=QueuedRetryStatuses.PENDING)

    task_type_id = Column(Integer, ForeignKey("task_type.task_type_id", ondelete="CASCADE"), nullable=False)

    task_type = relationship("TaskType", back_populates="retry_tasks", lazy=True)
    task_type_key_values = relationship("TaskTypeKeyValue", back_populates="retry_task", lazy=True)


class TaskType(Base, TimestampMixin):
    __tablename__ = "task_type"

    task_type_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    retry_tasks = relationship("RetryTask", back_populates="task_type", lazy=True)
    task_type_keys = relationship("TaskTypeKey", back_populates="task_type", lazy=True)


class TaskTypeKey(Base, TimestampMixin):
    __tablename__ = "task_type_key"

    task_type_key_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    type = Column(Enum(TaskParamsKeyTypes), nullable=False, default=TaskParamsKeyTypes.STRING)

    task_type_id = Column(Integer, ForeignKey("task_type.task_type_id", ondelete="CASCADE"), nullable=False)

    task_type = relationship("TaskType", back_populates="task_type_keys", lazy=True)
    task_type_key_values = relationship("TaskTypeKeyValue", back_populates="task_type_key", lazy=True)


class TaskTypeKeyValue(Base, TimestampMixin):
    __tablename__ = "task_type_key_value"

    value = Column(String, nullable=True)

    retry_task_id = Column(
        Integer,
        ForeignKey("retry_task.retry_task_id", ondelete="CASCADE"),
        primary_key=True,
    )
    task_type_key_id = Column(
        Integer,
        ForeignKey("task_type_key.task_type_key_id", ondelete="CASCADE"),
        primary_key=True,
    )

    task_type_key = relationship("TaskTypeKey", back_populates="task_type_key_values", lazy=True)
    retry_task = relationship("RetryTask", back_populates="task_type_key_values", lazy=True)
