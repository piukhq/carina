"""Add foreign key constraint constraint to filelog id

Revision ID: 43bdcbcd05d5
Revises: f0562d068211
Create Date: 2023-03-27 11:13:40.515011

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "43bdcbcd05d5"
down_revision = "f0562d068211"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("reward_reward_file_log_id_fkey", "reward", type_="foreignkey")
    op.create_foreign_key(
        "reward_reward_file_log_id_fkey",
        "reward",
        "reward_file_log",
        ["reward_file_log_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("reward_reward_file_log_id_fkey", "reward", type_="foreignkey")
    op.create_foreign_key("reward_reward_file_log_id_fkey", "reward", "reward_file_log", ["reward_file_log_id"], ["id"])
