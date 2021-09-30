"""added queued retry status waiting

Revision ID: 90d8283d0add
Revises: f21afdef225b
Create Date: 2021-09-30 09:46:31.188334

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "90d8283d0add"
down_revision = "f21afdef225b"
branch_labels = None
depends_on = None

old_options = ("PENDING", "IN_PROGRESS", "FAILED", "SUCCESS")
new_options = sorted(old_options + ("WAITING",))

old_type = sa.Enum(*old_options, name="queuedretrystatuses")
new_type = sa.Enum(*new_options, name="queuedretrystatuses")
tmp_type = sa.Enum(*new_options, name="_queuedretrystatuses")

tva = sa.sql.table("voucher_allocation", sa.Column("status", new_type, nullable=False))
tvu = sa.sql.table("voucher_update", sa.Column("retry_status", new_type, nullable=False))


def upgrade() -> None:
    # Create a temporary "_queuedretrystatuses" type, convert and drop the "old" type
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE voucher_allocation ALTER COLUMN status TYPE _queuedretrystatuses"
        " USING status::text::_queuedretrystatuses"
    )
    op.execute(
        "ALTER TABLE voucher_update ALTER COLUMN retry_status TYPE _queuedretrystatuses"
        " USING retry_status::text::_queuedretrystatuses"
    )
    old_type.drop(op.get_bind(), checkfirst=False)
    # Create and convert to the "new" status type
    new_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE voucher_allocation ALTER COLUMN status TYPE queuedretrystatuses"
        " USING status::text::queuedretrystatuses"
    )
    op.execute(
        "ALTER TABLE voucher_update ALTER COLUMN retry_status TYPE queuedretrystatuses"
        " USING retry_status::text::queuedretrystatuses"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)


def downgrade() -> None:
    # Convert 'WAITING' status into 'FAILED'
    op.execute(tva.update().where(tva.c.status == u"WAITING").values(status="FAILED"))
    op.execute(tvu.update().where(tvu.c.retry_status == u"WAITING").values(retry_status="FAILED"))
    # Create a temporary "_queuedretrystatuses" type, convert and drop the "new" type
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE voucher_allocation ALTER COLUMN status TYPE _queuedretrystatuses"
        " USING status::text::_queuedretrystatuses"
    )
    op.execute(
        "ALTER TABLE voucher_update ALTER COLUMN retry_status TYPE _queuedretrystatuses"
        " USING retry_status::text::_queuedretrystatuses"
    )
    new_type.drop(op.get_bind(), checkfirst=False)
    # Create and convert to the "old" status type
    old_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE voucher_allocation ALTER COLUMN status TYPE queuedretrystatuses"
        " USING status::text::queuedretrystatuses"
    )
    op.execute(
        "ALTER TABLE voucher_update ALTER COLUMN retry_status TYPE queuedretrystatuses"
        " USING retry_status::text::queuedretrystatuses"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)
