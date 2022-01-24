"""issuance error handler

Revision ID: 8f879d830ac9
Revises: ad0bbf13ad98
Create Date: 2021-12-08 12:06:47.992602

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "8f879d830ac9"
down_revision = "ad0bbf13ad98"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    task_type = sa.Table("task_type", sa.MetaData(), autoload_with=conn)
    conn.execute(
        sa.update(task_type)
        .where(task_type.c.name == "voucher-issuance")
        .values(error_handler_path="app.tasks.error_handlers.handle_issue_voucher_request_error")
    )


def downgrade() -> None:
    conn = op.get_bind()
    task_type = sa.Table("task_type", sa.MetaData(), autoload_with=conn)
    conn.execute(
        sa.update(task_type)
        .where(task_type.c.name == "voucher-issuance")
        .values(error_handler_path="app.tasks.error_handlers.handle_retry_task_request_error")
    )
