"""update allocation module name

Revision ID: be271e924073
Revises: 9980df342054
Create Date: 2021-11-03 12:42:13.059319

"""
import sqlalchemy as sa

from alembic import op
from app.core.config import settings

# revision identifiers, used by Alembic.
revision = "be271e924073"
down_revision = "9980df342054"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.get_bind().execute(
        sa.text("UPDATE task_type SET path = :action_path WHERE name = :task_name"),
        action_path="app.tasks.issuance.issue_voucher",
        task_name="voucher-issuance",
    )


def downgrade() -> None:
    op.get_bind().execute(
        sa.text("UPDATE task_type SET path = :action_path WHERE name = :task_name"),
        action_path="app.tasks.allocation.issue_voucher",
        task_name="voucher-issuance",
    )
