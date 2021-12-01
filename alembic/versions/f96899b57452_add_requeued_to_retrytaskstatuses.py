"""add requeued to retrytaskstatuses

Revision ID: f96899b57452
Revises: 15d687a1f3f2
Create Date: 2021-11-30 09:35:55.869248

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "f96899b57452"
down_revision = "15d687a1f3f2"
branch_labels = None
depends_on = None


old_options = ("PENDING", "IN_PROGRESS", "FAILED", "SUCCESS", "WAITING", "CANCELLED")
new_options = old_options + ("REQUEUED",)

old_type = sa.Enum(*old_options, name="retrytaskstatuses")
new_type = sa.Enum(*new_options, name="retrytaskstatuses")
tmp_type = sa.Enum(*new_options, name="_retrytaskstatuses")

retry_task_table = sa.sql.table("retry_task", sa.Column("status", new_type, nullable=False))


def upgrade() -> None:
    # Create a tempoary "_retrytasksstatuses" type, convert and drop the "old" type
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE retry_task ALTER COLUMN status TYPE _retrytaskstatuses USING status::text::_retrytaskstatuses"
    )
    old_type.drop(op.get_bind(), checkfirst=False)
    # Create and convert to the "new" status type
    new_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE retry_task ALTER COLUMN status TYPE retrytaskstatuses USING status::text::retrytaskstatuses"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)


def downgrade() -> None:
    # Convert 'REQUEUED' status into 'FAILED'
    op.execute(retry_task_table.update().where(retry_task_table.c.status == u"REQUEUED").values(status="FAILED"))
    # Create a tempoary "_retrytaskstatuses" type, convert and drop the "new" type
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE retry_task ALTER COLUMN status TYPE _retrytaskstatuses USING status::text::_retrytaskstatuses"
    )
    new_type.drop(op.get_bind(), checkfirst=False)
    # Create and convert to the "old" status type
    old_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE retry_task ALTER COLUMN status TYPE retrytaskstatuses USING status::text::retrytaskstatuses"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)
