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


old = "app.tasks"
new = "carina.tasks"


def upgrade() -> None:
    conn = op.get_bind()
    metadata = sa.MetaData()
    TaskType = sa.Table("task_type", metadata, autoload_with=conn)
    conn.execute(TaskType.update(values={TaskType.c.path: sa.func.replace(TaskType.c.path, old, new)}))
    conn.execute(
        TaskType.update(
            values={TaskType.c.error_handler_path: sa.func.replace(TaskType.c.error_handler_path, old, new)}
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    metadata = sa.MetaData()
    TaskType = sa.Table("task_type", metadata, autoload_with=conn)
    conn.execute(TaskType.update(values={TaskType.c.path: sa.func.replace(TaskType.c.path, new, old)}))
    conn.execute(
        TaskType.update(
            values={TaskType.c.error_handler_path: sa.func.replace(TaskType.c.error_handler_path, new, old)}
        )
    )
