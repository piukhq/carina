"""delete unallocated vouchers task type

Revision ID: ad0bbf13ad98
Revises: 4282380cb010
Create Date: 2021-11-26 14:30:01.462340

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "ad0bbf13ad98"
down_revision = "4282380cb010"
branch_labels = None
depends_on = None


def get_tables(conn: sa.engine.Connection) -> tuple[sa.Table, sa.Table]:
    metadata = sa.MetaData()
    task_type = sa.Table("task_type", metadata, autoload_with=conn)
    task_type_key = sa.Table("task_type_key", metadata, autoload_with=conn)

    return task_type, task_type_key


def upgrade() -> None:
    conn = op.get_bind()
    task_type, task_type_key = get_tables(conn)

    inserted_obj = conn.execute(
        sa.insert(task_type).values(
            name="delete-unallocated-vouchers",
            path="app.tasks.voucher_deletion.delete_unallocated_vouchers",
            error_handler_path="app.tasks.error_handlers.default_handler",
            queue_name="carina:default",
        )
    )
    task_type_id = inserted_obj.inserted_primary_key[0]
    op.bulk_insert(
        task_type_key,
        [
            {"task_type_id": task_type_id} | task_type_key_data
            for task_type_key_data in (
                {"name": "retailer_slug", "type": "STRING"},
                {"name": "voucher_type_slug", "type": "STRING"},
            )
        ],
    )


def downgrade() -> None:
    conn = op.get_bind()
    task_type, _ = get_tables(conn)
    conn.execute(task_type.delete().where(task_type.c.name == "delete-unallocated-vouchers"))
