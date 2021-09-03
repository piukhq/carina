"""voucher table

Revision ID: 6ee302570436
Revises: 94cfb5b593f3
Create Date: 2021-07-21 11:13:28.742905

"""
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "6ee302570436"
down_revision = "94cfb5b593f3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "voucher",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("voucher_code", sa.String(), nullable=False),
        sa.Column("allocated", sa.Boolean(), nullable=False),
        sa.Column("voucher_config_id", sa.Integer(), nullable=False),
        sa.Column("retailer_slug", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["voucher_config_id"],
            ["voucher_config.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("voucher_code", "retailer_slug", name="voucher_code_retailer_slug_unq"),
    )
    op.create_index(op.f("ix_voucher_retailer_slug"), "voucher", ["retailer_slug"], unique=False)
    op.create_index(op.f("ix_voucher_voucher_code"), "voucher", ["voucher_code"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_voucher_voucher_code"), table_name="voucher")
    op.drop_index(op.f("ix_voucher_retailer_slug"), table_name="voucher")
    op.drop_table("voucher")
