"""change voucher update table to use voucher foreign key

Revision ID: f9f0851df16c
Revises: a757c187a9aa
Create Date: 2021-09-06 11:19:08.158178

"""
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "f9f0851df16c"
down_revision = "a757c187a9aa"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("voucher_update", sa.Column("voucher_id", postgresql.UUID(as_uuid=True), nullable=False))
    op.execute(
        "UPDATE voucher_update SET voucher_id = voucher.id FROM voucher WHERE voucher_update.retailer_slug = "
        "voucher.retailer_slug AND voucher_update.voucher_code = voucher.voucher_code"
    )
    op.drop_index("ix_voucher_update_retailer_slug", table_name="voucher_update")
    op.drop_index("ix_voucher_update_voucher_code", table_name="voucher_update")
    op.create_foreign_key(
        "voucher_update_voucher_id_fkey", "voucher_update", "voucher", ["voucher_id"], ["id"], ondelete="CASCADE"
    )
    op.drop_column("voucher_update", "retailer_slug")
    op.drop_column("voucher_update", "voucher_code")


def downgrade() -> None:
    op.add_column("voucher_update", sa.Column("voucher_code", sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column(
        "voucher_update", sa.Column("retailer_slug", sa.VARCHAR(length=32), autoincrement=False, nullable=True)
    )
    op.execute(
        "UPDATE voucher_update SET voucher_code = voucher.voucher_code, retailer_slug = voucher.retailer_slug "
        "FROM voucher WHERE voucher_update.voucher_id = voucher.id"
    )
    op.alter_column("voucher_update", "voucher_code", nullable=False)
    op.alter_column("voucher_update", "retailer_slug", nullable=False)
    op.create_index("ix_voucher_update_voucher_code", "voucher_update", ["voucher_code"], unique=False)
    op.create_index("ix_voucher_update_retailer_slug", "voucher_update", ["retailer_slug"], unique=False)
    op.drop_constraint("voucher_update_voucher_id_fkey", "voucher_update", type_="foreignkey")
    op.drop_column("voucher_update", "voucher_id")
