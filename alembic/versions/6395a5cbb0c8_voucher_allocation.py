"""voucher allocation

Revision ID: 6395a5cbb0c8
Revises: 6ee302570436
Create Date: 2021-07-28 12:21:06.906430

"""
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "6395a5cbb0c8"
down_revision = "6ee302570436"
branch_labels = None
depends_on = None

voucherallocationstatuses = sa.Enum("PENDING", "IN_PROGRESS", "FAILED", "SUCCESS", name="voucherallocationstatuses")
voucherfetchtype = sa.Enum("PRE_ALLOCATED", name="voucherfetchtype")


def upgrade() -> None:
    bind = op.get_bind()
    op.create_table(
        "voucher_allocation",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("status", voucherallocationstatuses, nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("account_url", sa.String(), nullable=False),
        sa.Column("issued_date", sa.Integer(), nullable=False),
        sa.Column("expiry_date", sa.Integer(), nullable=True),
        sa.Column("next_attempt_time", sa.DateTime(), nullable=True),
        sa.Column("response_data", postgresql.JSONB(astext_type=sa.Text()), nullable=False),  # type: ignore [call-arg]
        sa.Column("voucher_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("voucher_config_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["voucher_config_id"], ["voucher_config.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["voucher_id"], ["voucher.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_voucher_allocation_id"), "voucher_allocation", ["id"], unique=False)
    voucherfetchtype.create(bind, checkfirst=False)
    op.add_column("voucher_config", sa.Column("fetch_type", voucherfetchtype, nullable=True))
    op.execute("UPDATE voucher_config SET fetch_type='PRE_ALLOCATED'")
    op.alter_column("voucher_config", "fetch_type", nullable=False)


def downgrade() -> None:
    bind = op.get_bind()
    op.drop_column("voucher_config", "fetch_type")
    op.drop_index(op.f("ix_voucher_allocation_id"), table_name="voucher_allocation")
    op.drop_table("voucher_allocation")
    voucherfetchtype.drop(bind, checkfirst=False)
    voucherallocationstatuses.drop(bind, checkfirst=False)
