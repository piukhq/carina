"""Add reward_file_log to reward table

Revision ID: f0562d068211
Revises: 605e6656b0b4
Create Date: 2023-03-15 10:47:27.357797

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "f0562d068211"
down_revision = "605e6656b0b4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("reward", sa.Column("reward_file_log_id", sa.Integer(), nullable=True))
    op.create_foreign_key(None, "reward", "reward_file_log", ["reward_file_log_id"], ["id"])


def downgrade() -> None:
    op.drop_column("reward", "reward_file_log_id")
