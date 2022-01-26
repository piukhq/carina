"""change issue reward error handler

Revision ID: 76d499217f16
Revises: 69cc1d0099e4
Create Date: 2022-01-26 10:50:52.296535

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "76d499217f16"
down_revision = "69cc1d0099e4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    task_type = sa.Table("task_type", sa.MetaData(), autoload_with=conn)
    conn.execute(
        sa.update(task_type)
        .where(task_type.c.name == "reward-issuance")
        .values(error_handler_path="app.tasks.error_handlers.handle_issue_reward_request_error")
    )


def downgrade() -> None:
    conn = op.get_bind()
    task_type = sa.Table("task_type", sa.MetaData(), autoload_with=conn)
    conn.execute(
        sa.update(task_type)
        .where(task_type.c.name == "reward-issuance")
        .values(error_handler_path="app.tasks.error_handlers.handle_issue_voucher_request_error")
    )
