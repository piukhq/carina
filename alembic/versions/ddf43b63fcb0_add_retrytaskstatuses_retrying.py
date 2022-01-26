"""add RetryTaskStatuses.RETRYING

Revision ID: ddf43b63fcb0
Revises: af1ac6c1c854
Create Date: 2022-01-20 13:43:13.819403

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "ddf43b63fcb0"
down_revision = "af1ac6c1c854"
branch_labels = None
depends_on = None

old_options = ("PENDING", "IN_PROGRESS", "FAILED", "SUCCESS", "WAITING", "CANCELLED", "REQUEUED")
new_options = old_options + ("RETRYING",)

old_type = sa.Enum(*old_options, name="retrytaskstatuses")
new_type = sa.Enum(*new_options, name="retrytaskstatuses")
tmp_type = sa.Enum(*new_options, name="_retrytaskstatuses")

retry_task_table = sa.sql.table("retry_task", sa.Column("status", new_type, nullable=False))


def upgrade() -> None:
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE retry_task ALTER COLUMN status TYPE _retrytaskstatuses USING status::text::_retrytaskstatuses"
    )
    old_type.drop(op.get_bind(), checkfirst=False)
    new_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE retry_task ALTER COLUMN status TYPE retrytaskstatuses USING status::text::retrytaskstatuses"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)


def downgrade() -> None:
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE retry_task ALTER COLUMN status TYPE _retrytaskstatuses USING status::text::_retrytaskstatuses"
    )
    new_type.drop(op.get_bind(), checkfirst=False)
    old_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE retry_task ALTER COLUMN status TYPE retrytaskstatuses USING status::text::retrytaskstatuses"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)
