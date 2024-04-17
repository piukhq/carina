"""add retry_task.status index

Revision ID: 4c2e02b5bc03
Revises: 2b5f2e762b35
Create Date: 2022-04-20 22:00:53.982689

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "4c2e02b5bc03"
down_revision = "2b5f2e762b35"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(op.f("ix_retry_task_status"), "retry_task", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_retry_task_status"), table_name="retry_task")
