"""add cleanup handler path

Revision ID: 240181573092
Revises: f24aca10cfd7
Create Date: 2022-10-04 14:45:05.902330

"""
import sqlalchemy as sa

from alembic import op
from sqlalchemy import update

# revision identifiers, used by Alembic.
revision = "240181573092"
down_revision = "d092833784d0"
branch_labels = None
depends_on = None

reward_issuance_task_name = "reward-issuance"


old_options = ("PENDING", "IN_PROGRESS", "RETRYING", "FAILED", "SUCCESS", "WAITING", "CANCELLED", "REQUEUED")
new_options = (
    "PENDING",
    "IN_PROGRESS",
    "RETRYING",
    "FAILED",
    "SUCCESS",
    "WAITING",
    "CANCELLED",
    "REQUEUED",
    "CLEANUP",
    "CLEANUP_FAILED",
)
enum_name = "retrytaskstatuses"
old_type = sa.Enum(*old_options, name=enum_name)
new_type = sa.Enum(*new_options, name=enum_name)
tmp_type = sa.Enum(*new_options, name="_retrytaskstatuses_old")


def upgrade() -> None:
    conn = op.get_bind()
    # ADD CLEANUP TO ENUM
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE retry_task ALTER COLUMN status TYPE _retrytaskstatuses_old USING status::text::_retrytaskstatuses_old"
    )
    old_type.drop(op.get_bind(), checkfirst=False)
    new_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE retry_task ALTER COLUMN status TYPE retrytaskstatuses USING status::text::retrytaskstatuses"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)

    # ADD COLUMN TO TASKTYPE AND ADD PATH FOR REWARD ISSUANCE TASK
    metadata = sa.MetaData()
    op.add_column("task_type", sa.Column("cleanup_handler_path", sa.String(), nullable=True))

    TaskType = sa.Table("task_type", metadata, autoload_with=conn)
    conn.execute(
        TaskType.update(
            values={TaskType.c.cleanup_handler_path: "carina.tasks.cleanup_handlers.reward_issuance_cleanup_handler"}
        ).where(TaskType.c.name == reward_issuance_task_name)
    )


def downgrade() -> None:
    # REVERT ENUM
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE retry_task ALTER COLUMN status TYPE _retrytaskstatuses_old USING status::text::_retrytaskstatuses_old"
    )
    new_type.drop(op.get_bind(), checkfirst=False)
    old_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE retry_task ALTER COLUMN status TYPE retrytaskstatuses USING status::text::retrytaskstatuses"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)

    # DROP NEW COLUMN ON TASKTYPE TABLE
    op.drop_column("task_type", "cleanup_handler_path")
