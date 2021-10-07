"""voucher config status

Revision ID: 8b8f036e34ef
Revises: 90d8283d0add
Create Date: 2021-10-04 16:52:10.944582

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "8b8f036e34ef"
down_revision = "90d8283d0add"
branch_labels = None
depends_on = None

vouchertypestatuses = sa.Enum("ACTIVE", "CANCELLED", "ENDED", name="vouchertypestatuses")


def upgrade() -> None:
    vouchertypestatuses.create(op.get_bind(), checkfirst=False)
    op.add_column("voucher_config", sa.Column("status", vouchertypestatuses, nullable=True))
    op.execute("UPDATE voucher_config SET status = 'ACTIVE'")
    op.alter_column("voucher_config", "status", nullable=False)


def downgrade() -> None:
    op.drop_column("voucher_config", "status")
    vouchertypestatuses.drop(op.get_bind(), checkfirst=False)
