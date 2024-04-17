"""issuance campaign slug key

Revision ID: d092833784d0
Revises: f24aca10cfd7
Create Date: 2022-10-06 12:32:36.841833

"""
from typing import Any

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "d092833784d0"
down_revision = "f24aca10cfd7"
branch_labels = None
depends_on = None


reward_issuance_task_name = "reward-issuance"
new_key_type_data = {"name": "campaign_slug", "type": "STRING"}


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
