"""merge workers

Revision ID: 9980df342054
Revises: 4731e2ed8bac
Create Date: 2021-11-03 12:21:17.037014

"""
import sqlalchemy as sa

from alembic import op
from app.core.config import settings
from app.tasks.error_handlers import handle_retry_task_request_error

# revision identifiers, used by Alembic.
revision = "9980df342054"
down_revision = "4731e2ed8bac"
branch_labels = None
depends_on = None


def _get_table_and_connection() -> tuple[sa.engine.Connection, sa.Table]:
    metadata = sa.MetaData()
    conn = op.get_bind()
    task_type = sa.Table("task_type", metadata, autoload_with=conn)
    return conn, task_type


def set_error_handler_path_and_update_queue_name() -> None:
    conn, task_type = _get_table_and_connection()
    task_names = (settings.VOUCHER_ISSUANCE_TASK_NAME, settings.VOUCHER_STATUS_ADJUSTMENT_TASK_NAME)
    conn.execute(
        sa.update(task_type)
        .where(task_type.c.name.in_(task_names))
        .values(
            queue_name="carina:default",
            error_handler_path=(
                handle_retry_task_request_error.__module__ + "." + handle_retry_task_request_error.__name__
            ),
        )
    )


def revert_queue_name_update() -> None:
    conn, task_type = _get_table_and_connection()
    for task_name, queue_name in (
        (settings.VOUCHER_ISSUANCE_TASK_NAME, "bpl_voucher_issuance"),
        (settings.VOUCHER_STATUS_ADJUSTMENT_TASK_NAME, "bpl_voucher_status_update"),
    ):
        conn.execute(sa.update(task_type).where(task_type.c.name == task_name).values(queue_name=queue_name))


def upgrade() -> None:
    op.add_column("task_type", sa.Column("error_handler_path", sa.String(), nullable=True))
    set_error_handler_path_and_update_queue_name()
    op.alter_column("task_type", "error_handler_path", nullable=False)


def downgrade() -> None:
    op.drop_column("task_type", "error_handler_path")
    revert_queue_name_update()
