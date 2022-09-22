"""alter task paths

Revision ID: f24aca10cfd7
Revises: ab2a70f485d9
Create Date: 2022-09-21 17:17:25.728354

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "f24aca10cfd7"
down_revision = "ab2a70f485d9"
branch_labels = None
depends_on = None


tasks_old = "app.tasks"
tasks_new = "carina.tasks"
fetch_type_old = "app.fetch_reward"
fetch_type_new = "carina.fetch_reward"


def upgrade() -> None:
    conn = op.get_bind()
    metadata = sa.MetaData()
    TaskType = sa.Table("task_type", metadata, autoload_with=conn)
    conn.execute(TaskType.update(values={TaskType.c.path: sa.func.replace(TaskType.c.path, tasks_old, tasks_new)}))
    conn.execute(
        TaskType.update(
            values={TaskType.c.error_handler_path: sa.func.replace(TaskType.c.error_handler_path, tasks_old, tasks_new)}
        )
    )
    FetchType = sa.Table("fetch_type", metadata, autoload_with=conn)
    conn.execute(
        FetchType.update(values={FetchType.c.path: sa.func.replace(FetchType.c.path, fetch_type_old, fetch_type_new)})
    )


def downgrade() -> None:
    conn = op.get_bind()
    metadata = sa.MetaData()
    TaskType = sa.Table("task_type", metadata, autoload_with=conn)
    conn.execute(TaskType.update(values={TaskType.c.path: sa.func.replace(TaskType.c.path, tasks_new, tasks_old)}))
    conn.execute(
        TaskType.update(
            values={TaskType.c.error_handler_path: sa.func.replace(TaskType.c.error_handler_path, tasks_new, tasks_old)}
        )
    )
    FetchType = sa.Table("fetch_type", metadata, autoload_with=conn)
    conn.execute(
        FetchType.update(values={FetchType.c.path: sa.func.replace(FetchType.c.path, fetch_type_new, fetch_type_old)})
    )
