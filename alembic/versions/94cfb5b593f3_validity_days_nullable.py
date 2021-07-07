"""validity days nullable

Revision ID: 94cfb5b593f3
Revises: ae77166b799f
Create Date: 2021-07-07 14:44:27.967261

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "94cfb5b593f3"
down_revision = "ae77166b799f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("voucher_config", "validity_days", existing_type=sa.INTEGER(), nullable=True)


def downgrade() -> None:
    op.execute("UPDATE voucher_config SET validity_days = 0 WHERE validity_days IS NULL")
    op.alter_column("voucher_config", "validity_days", existing_type=sa.INTEGER(), nullable=False)
