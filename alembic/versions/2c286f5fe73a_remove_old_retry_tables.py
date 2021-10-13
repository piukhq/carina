"""remove old retry tables

Revision ID: 2c286f5fe73a
Revises: 35b969f1e965
Create Date: 2021-10-05 12:44:43.320533

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "2c286f5fe73a"
down_revision = "35b969f1e965"
branch_labels = None
depends_on = None


def upgrade() -> None:
    queuedretrystatuses = sa.Enum("PENDING", "IN_PROGRESS", "FAILED", "SUCCESS", name="queuedretrystatuses")
    op.drop_table("voucher_allocation")
    op.drop_index("ix_voucher_config_id", table_name="voucher_config")
    op.drop_index("ix_voucher_update_id", table_name="voucher_update")
    op.drop_column("voucher_update", "retry_status")
    op.drop_column("voucher_update", "response_data")
    op.drop_column("voucher_update", "attempts")
    op.drop_column("voucher_update", "next_attempt_time")
    queuedretrystatuses.drop(bind=op.get_bind(), checkfirst=False)


def downgrade() -> None:
    raise NotImplementedError("non downgradeable migration.")
