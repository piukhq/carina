"""deleted field for voucher table

Revision ID: d46bf532324b
Revises: ef013ab3d4c0
Create Date: 2021-08-19 15:29:30.560608

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d46bf532324b"
down_revision = "ef013ab3d4c0"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("voucher", sa.Column("deleted", sa.Boolean(), nullable=True))
    op.execute("UPDATE voucher SET deleted=False")
    op.alter_column("voucher", "deleted", nullable=False)


def downgrade():
    op.drop_column("voucher", "deleted")
