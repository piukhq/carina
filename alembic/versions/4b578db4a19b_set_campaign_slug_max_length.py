"""set campaign slug max length

Revision ID: 4b578db4a19b
Revises: 2b2007c73ab0
Create Date: 2023-01-26 16:01:19.243314

"""
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "4b578db4a19b"
down_revision = "2b2007c73ab0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "reward_campaign",
        "campaign_slug",
        existing_type=sa.VARCHAR(length=32),
        type_=sa.String(length=120),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "reward_campaign",
        "campaign_slug",
        existing_type=sa.String(length=120),
        type_=sa.VARCHAR(length=32),
        existing_nullable=False,
    )
