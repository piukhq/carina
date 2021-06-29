"""init voucher config table

Revision ID: ae77166b799f
Revises: 
Create Date: 2021-06-28 17:25:35.536774

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "ae77166b799f"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "voucher_config",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("voucher_type_slug", sa.String(length=32), nullable=False),
        sa.Column("validity_days", sa.Integer(), nullable=False),
        sa.Column("retailer_slug", sa.String(length=32), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_voucher_config_id"), "voucher_config", ["id"], unique=False)
    op.create_index(op.f("ix_voucher_config_retailer_slug"), "voucher_config", ["retailer_slug"], unique=False)
    op.create_index(op.f("ix_voucher_config_voucher_type_slug"), "voucher_config", ["voucher_type_slug"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_voucher_config_voucher_type_slug"), table_name="voucher_config")
    op.drop_index(op.f("ix_voucher_config_retailer_slug"), table_name="voucher_config")
    op.drop_index(op.f("ix_voucher_config_id"), table_name="voucher_config")
    op.drop_table("voucher_config")
