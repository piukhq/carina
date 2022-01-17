"""Rename enumeration and update task_type name to reward-status-adjustment

Revision ID: c5ddcd6a12d7
Revises: 9589c7115a36
Create Date: 2022-01-11 13:04:38.868974

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c5ddcd6a12d7"
down_revision = "9589c7115a36"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE vouchertypestatuses RENAME TO rewardtypestatuses")
    op.execute("UPDATE task_type SET name = 'reward-status-adjustment' WHERE name = 'voucher-status-adjustment'")


def downgrade() -> None:
    op.execute("ALTER TYPE rewardtypestatuses RENAME TO vouchertypestatuses")
    op.execute("UPDATE task_type SET name = 'voucher-status-adjustment' WHERE name = 'reward-status-adjustment'")
