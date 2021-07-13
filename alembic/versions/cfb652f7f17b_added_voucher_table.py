"""Added voucher table

Revision ID: cfb652f7f17b
Revises: 94cfb5b593f3
Create Date: 2021-07-13 13:59:08.471748

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "cfb652f7f17b"
down_revision = "94cfb5b593f3"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
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
        sa.ForeignKeyConstraint(["voucher_config_id"], ["voucher_config.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_voucher_id"), "voucher", ["id"], unique=False)
    op.create_index(op.f("ix_voucher_voucher_code"), "voucher", ["voucher_code"], unique=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_voucher_voucher_code"), table_name="voucher")
    op.drop_index(op.f("ix_voucher_id"), table_name="voucher")
    op.drop_table("voucher")
    # ### end Alembic commands ###
