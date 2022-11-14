"""retailer status

Revision ID: ed227f6cc7e1
Revises: e763e2378670
Create Date: 2022-11-14 16:46:29.285597

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "ed227f6cc7e1"
down_revision = "e763e2378670"
branch_labels = None
depends_on = None

retailerstatuses = sa.Enum("TEST", "ACTIVE", "INACTIVE", "DELETED", "ARCHIVED", "SUSPENDED", name="retailerstatuses")


def upgrade() -> None:
    retailerstatuses.create(op.get_bind(), checkfirst=False)
    op.add_column("retailer", sa.Column("status", retailerstatuses, nullable=True))
    op.execute("UPDATE retailer SET status = 'TEST' WHERE status is NULL;")
    op.alter_column("retailer", "status", nullable=False)
    op.create_index(op.f("ix_retailer_status"), "retailer", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_retailer_status"), table_name="retailer")
    op.drop_column("retailer", "status")
    retailerstatuses.drop(op.get_bind(), checkfirst=False)
