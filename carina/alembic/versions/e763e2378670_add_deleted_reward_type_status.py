"""Add deleted reward_type_status

Revision ID: e763e2378670
Revises: 44b065b868a4
Create Date: 2022-10-19 10:45:02.867105

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "e763e2378670"
down_revision = "79f2450fae5e"
branch_labels = None
depends_on = None


old_options = ("ACTIVE", "CANCELLED", "ENDED")
new_options = ("ACTIVE", "CANCELLED", "ENDED", "DELETED")
enum_name = "rewardtypestatuses"

old_type = sa.Enum(*old_options, name=enum_name)
new_type = sa.Enum(*new_options, name=enum_name)
tmp_type = sa.Enum(*new_options, name="_rewardtypestatuses")


def upgrade() -> None:
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE reward_config ALTER COLUMN status TYPE _rewardtypestatuses USING status::text::_rewardtypestatuses"
    )
    old_type.drop(op.get_bind(), checkfirst=False)
    new_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE reward_config ALTER COLUMN status TYPE rewardtypestatuses USING status::text::rewardtypestatuses"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)


def downgrade() -> None:
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE reward_config ALTER COLUMN status TYPE _rewardtypestatuses USING status::text::_rewardtypestatuses"
    )
    new_type.drop(op.get_bind(), checkfirst=False)
    old_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE reward_config ALTER COLUMN status TYPE rewardtypestatuses USING status::text::rewardtypestatuses"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)
