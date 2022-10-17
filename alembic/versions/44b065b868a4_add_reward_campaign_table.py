"""add reward campaign table

Revision ID: 44b065b868a4
Revises: d092833784d0
Create Date: 2022-10-11 11:50:31.323646

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "44b065b868a4"
down_revision = "240181573092"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "reward_campaign",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("reward_slug", sa.String(length=32), nullable=False),
        sa.Column("campaign_slug", sa.String(length=32), nullable=False),
        sa.Column("retailer_id", sa.Integer(), nullable=False),
        sa.Column(
            "campaign_status",
            sa.Enum("ACTIVE", "CANCELLED", "DRAFT", "ENDED", name="rewardcampaignstatuses"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["retailer_id"], ["retailer.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("campaign_slug", "retailer_id", name="campaign_slug_retailer_unq"),
    )
    op.create_index(op.f("ix_reward_campaign_campaign_slug"), "reward_campaign", ["campaign_slug"], unique=False)
    op.create_index(op.f("ix_reward_campaign_reward_slug"), "reward_campaign", ["reward_slug"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_reward_campaign_reward_slug"), table_name="reward_campaign")
    op.drop_index(op.f("ix_reward_campaign_campaign_slug"), table_name="reward_campaign")
    op.drop_table("reward_campaign")
