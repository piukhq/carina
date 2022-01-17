"""Rename enumeration from VoucherUpdateStatuses to RewardUpdateStatuses

Revision ID: 9812a07aad99
Revises: c5ddcd6a12d7
Create Date: 2022-01-17 15:22:27.264081

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "9812a07aad99"
down_revision = "c5ddcd6a12d7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE voucherupdatestatuses RENAME TO rewardupdatestatuses")


def downgrade() -> None:
    op.execute("ALTER TYPE rewardupdatestatuses RENAME TO voucherupdatestatuses")
