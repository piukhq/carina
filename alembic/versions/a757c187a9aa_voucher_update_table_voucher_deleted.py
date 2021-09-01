"""voucher update table, voucher.deleted

Revision ID: a757c187a9aa
Revises: 6395a5cbb0c8
Create Date: 2021-08-26 12:47:05.005148

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "a757c187a9aa"
down_revision = "6395a5cbb0c8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "voucher_update",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("voucher_code", sa.String(), nullable=False),
        sa.Column("retailer_slug", sa.String(length=32), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("status", sa.Enum("ISSUED", "CANCELLED", "REDEEMED", name="voucherupdatestatuses"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_voucher_update_id"), "voucher_update", ["id"], unique=False)
    op.create_index(op.f("ix_voucher_update_retailer_slug"), "voucher_update", ["retailer_slug"], unique=False)
    op.create_index(op.f("ix_voucher_update_voucher_code"), "voucher_update", ["voucher_code"], unique=False)
    op.add_column("voucher", sa.Column("deleted", sa.Boolean(), nullable=True))
    op.execute("UPDATE voucher SET deleted=False")
    op.alter_column("voucher", "deleted", nullable=False)


def downgrade() -> None:
    op.drop_column("voucher", "deleted")
    op.drop_index(op.f("ix_voucher_update_voucher_code"), table_name="voucher_update")
    op.drop_index(op.f("ix_voucher_update_retailer_slug"), table_name="voucher_update")
    op.drop_index(op.f("ix_voucher_update_id"), table_name="voucher_update")
    op.drop_table("voucher_update")
    voucherupdatestatuses = sa.Enum(name="voucherupdatestatuses")
    voucherupdatestatuses.drop(op.get_bind(), checkfirst=False)
