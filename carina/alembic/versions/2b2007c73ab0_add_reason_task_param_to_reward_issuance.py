"""Add reason task param to reward-issuance

Revision ID: 2b2007c73ab0
Revises: ed227f6cc7e1
Create Date: 2022-11-21 10:59:12.318810

"""
from typing import Any

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "2b2007c73ab0"
down_revision = "ed227f6cc7e1"
branch_labels = None
depends_on = None


reward_adjustment_task_name = "reward-issuance"
key_type_list = [
    {"name": "reason", "type": "STRING"},
]


def get_table_and_subquery(conn: sa.engine.Connection) -> tuple[sa.Table, Any]:
    metadata = sa.MetaData()
    TaskType = sa.Table("task_type", metadata, autoload_with=conn)
    TaskTypeKey = sa.Table("task_type_key", metadata, autoload_with=conn)

    task_type_id_subquery = (
        sa.future.select(TaskType.c.task_type_id)
        .where(TaskType.c.name == reward_adjustment_task_name)
        .scalar_subquery()
    )

    return TaskTypeKey, task_type_id_subquery


def upgrade() -> None:
    conn = op.get_bind()
    TaskTypeKey, task_type_id_subquery = get_table_and_subquery(conn)
    conn.execute(
        TaskTypeKey.insert().values(task_type_id=task_type_id_subquery),
        key_type_list,
    )


def downgrade() -> None:
    conn = op.get_bind()
    TaskTypeKey, task_type_id_subquery = get_table_and_subquery(conn)
    conn.execute(
        TaskTypeKey.delete().where(
            TaskTypeKey.c.task_type_id == task_type_id_subquery,
            TaskTypeKey.c.name.in_([key["name"] for key in key_type_list]),
        )
    )
