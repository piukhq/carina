"""set campaign slug max length (again)

Revision ID: 605e6656b0b4
Revises: 4b578db4a19b
Create Date: 2023-01-27 09:23:44.603335

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "605e6656b0b4"
down_revision = "4b578db4a19b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "reward_campaign",
        "campaign_slug",
        existing_type=sa.VARCHAR(length=120),
        type_=sa.String(length=100),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "reward_campaign",
        "campaign_slug",
        existing_type=sa.String(length=100),
        type_=sa.VARCHAR(length=120),
        existing_nullable=False,
    )
