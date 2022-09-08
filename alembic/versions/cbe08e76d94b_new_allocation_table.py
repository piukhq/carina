"""new allocation table

Revision ID: cbe08e76d94b
Revises: c62b5f22a84d
Create Date: 2022-09-05 12:59:36.582306

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "cbe08e76d94b"
down_revision = "c62b5f22a84d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "allocation",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("idempotency_token", sa.String(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("account_url", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_token", name="idempotency_token_reward_allocation_unq"),
    )


def downgrade() -> None:
    op.drop_table("allocation")
