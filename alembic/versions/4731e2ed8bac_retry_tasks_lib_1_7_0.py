"""retry tasks lib 1.7.0 task type queue_name

Revision ID: 4731e2ed8bac
Revises: adbed3358eaf
Create Date: 2021-10-28 11:41:14.195572

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "4731e2ed8bac"
down_revision = "adbed3358eaf"
branch_labels = None
depends_on = None


def populate_existing_tasks() -> None:
    conn = op.get_bind()
    task_type = sa.Table("task_type", sa.MetaData(), autoload_with=conn)

    conn.execute(
        sa.update(task_type)
        .where(task_type.c.name == "voucher_issuance")
        .values(name="voucher-issuance", queue_name="bpl_voucher_issuance")
    )
    conn.execute(
        sa.update(task_type)
        .where(task_type.c.name == "voucher_status_adjustment")
        .values(name="voucher-status-adjustment", queue_name="bpl_voucher_status_update")
    )


def upgrade() -> None:
    op.add_column("task_type", sa.Column("queue_name", sa.String(), nullable=True))
    populate_existing_tasks()
    op.alter_column("task_type", "queue_name", nullable=False)


def downgrade() -> None:
    op.drop_column("task_type", "queue_name")
