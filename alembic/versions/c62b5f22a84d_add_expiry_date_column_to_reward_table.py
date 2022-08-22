"""Add expiry date column to Reward table

Revision ID: c62b5f22a84d
Revises: 4c2e02b5bc03
Create Date: 2022-08-16 11:25:02.288625

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c62b5f22a84d"
down_revision = "4c2e02b5bc03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("reward", sa.Column("expiry_date", sa.Date(), nullable=True))


def downgrade() -> None:
    op.drop_column("reward", "expiry_date")
