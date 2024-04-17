"""retailer slug issuance task key

Revision ID: ab2a70f485d9
Revises: 43ce4505d013
Create Date: 2022-09-14 13:32:45.644588

"""
from typing import Any

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "ab2a70f485d9"
down_revision = "43ce4505d013"
branch_labels = None
depends_on = None


reward_issuance_task_name = "reward-issuance"
new_key_type_data = {"name": "retailer_slug", "type": "STRING"}


def get_table_and_subquery(conn: sa.engine.Connection) -> tuple[sa.Table, Any]:
    metadata = sa.MetaData()
    TaskType = sa.Table("task_type", metadata, autoload_with=conn)
    TaskTypeKey = sa.Table("task_type_key", metadata, autoload_with=conn)

    task_type_id_subquery = (
        sa.future.select(TaskType.c.task_type_id).where(TaskType.c.name == reward_issuance_task_name).scalar_subquery()
    )

    return TaskTypeKey, task_type_id_subquery


def upgrade() -> None:
    conn = op.get_bind()
    TaskTypeKey, task_type_id_subquery = get_table_and_subquery(conn)
    conn.execute(TaskTypeKey.insert().values(task_type_id=task_type_id_subquery, **new_key_type_data))


def downgrade() -> None:
    conn = op.get_bind()
    TaskTypeKey, task_type_id_subquery = get_table_and_subquery(conn)
    conn.execute(
        TaskTypeKey.delete().where(
            TaskTypeKey.c.task_type_id == task_type_id_subquery,
            TaskTypeKey.c.name == new_key_type_data["name"],
        )
    )
