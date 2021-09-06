"""voucher update queue

Revision ID: f21afdef225b
Revises: a757c187a9aa
Create Date: 2021-09-03 17:50:09.956338

"""
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "f21afdef225b"
down_revision = "a757c187a9aa"
branch_labels = None
depends_on = None

old_type = sa.Enum("PENDING", "IN_PROGRESS", "FAILED", "SUCCESS", name="voucherallocationstatuses")
new_type = sa.Enum("PENDING", "IN_PROGRESS", "FAILED", "SUCCESS", name="queuedretrystatuses")


def upgrade() -> None:
    new_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE voucher_allocation ALTER COLUMN status TYPE queuedretrystatuses"
        " USING status::text::queuedretrystatuses"
    )
    old_type.drop(op.get_bind(), checkfirst=False)
    op.add_column("voucher_update", sa.Column("voucher_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("voucher_update", sa.Column("retry_status", new_type, nullable=True, default="PENDING"))
    op.execute("UPDATE voucher_update SET retry_status = 'FAILED' WHERE retry_status IS NULL")
    op.alter_column("voucher_update", "retry_status", nullable=False)
    op.add_column("voucher_update", sa.Column("attempts", sa.Integer(), nullable=True))
    op.execute("UPDATE voucher_update SET attempts = 0 WHERE attempts IS NULL")
    op.alter_column("voucher_update", "attempts", nullable=False)
    op.add_column("voucher_update", sa.Column("next_attempt_time", sa.DateTime(), nullable=True))
    op.add_column(
        "voucher_update",
        sa.Column(
            "response_data",
            postgresql.JSONB(astext_type=sa.Text()),  # type: ignore [call-arg]
            nullable=True,
        ),
    )
    op.execute("UPDATE voucher_update SET response_data = '{}' WHERE response_data IS NULL")
    op.alter_column("voucher_update", "response_data", nullable=False)


def downgrade() -> None:
    old_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE voucher_allocation ALTER COLUMN status TYPE voucherallocationstatuses"
        " USING status::text::voucherallocationstatuses"
    )
    op.drop_column("voucher_update", "voucher_id")
    op.drop_column("voucher_update", "response_data")
    op.drop_column("voucher_update", "next_attempt_time")
    op.drop_column("voucher_update", "attempts")
    op.drop_column("voucher_update", "retry_status")
    new_type.drop(op.get_bind(), checkfirst=False)
