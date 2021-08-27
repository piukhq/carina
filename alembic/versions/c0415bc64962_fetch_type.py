"""fetch type

Revision ID: c0415bc64962
Revises: 6395a5cbb0c8
Create Date: 2021-08-26 15:48:42.927431

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c0415bc64962"
down_revision = "6395a5cbb0c8"
branch_labels = None
depends_on = None


old_type = sa.Enum("PRE_ALLOCATED", name="voucherfetchtype")
new_type = sa.Enum("PRE_LOADED", name="voucherfetchtype")
tmp_type = sa.Enum("PRE_ALLOCATED", "PRE_LOADED", name="_voucherfetchtype")


def upgrade() -> None:
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE voucher_config ALTER COLUMN fetch_type TYPE _voucherfetchtype"
        " USING fetch_type::text::_voucherfetchtype"
    )
    op.execute("UPDATE voucher_config SET fetch_type = 'PRE_LOADED' where fetch_type = 'PRE_ALLOCATED'")
    old_type.drop(op.get_bind(), checkfirst=False)
    new_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE voucher_config ALTER COLUMN fetch_type TYPE voucherfetchtype"
        " USING fetch_type::text::voucherfetchtype"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)


def downgrade() -> None:
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE voucher_config ALTER COLUMN fetch_type TYPE _voucherfetchtype"
        " USING fetch_type::text::_voucherfetchtype"
    )
    op.execute("UPDATE voucher_config SET fetch_type = 'PRE_ALLOCATED' where fetch_type = 'PRE_LOADED'")
    new_type.drop(op.get_bind(), checkfirst=False)
    old_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE voucher_config ALTER COLUMN fetch_type TYPE voucherfetchtype"
        " USING fetch_type::text::voucherfetchtype"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)
