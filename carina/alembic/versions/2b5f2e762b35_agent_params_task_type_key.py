"""agent params task type key

Revision ID: 2b5f2e762b35
Revises: 629d5ac2463f
Create Date: 2022-03-21 12:09:30.934929

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "2b5f2e762b35"
down_revision = "629d5ac2463f"
branch_labels = None
depends_on = None

TASK_TYPE_NAME = "reward-issuance"
TASK_TYPE_KEY_NAME_OLD = "customer_card_ref"
TASK_TYPE_KEY_NAME_NEW = "agent_state_params_raw"


def get_tables(conn: sa.engine.Connection) -> tuple[sa.Table, sa.Table, sa.Table]:
    metadata = sa.MetaData()
    return (
        sa.Table("task_type", metadata, autoload_with=conn),
        sa.Table("task_type_key", metadata, autoload_with=conn),
        sa.Table("task_type_key_value", metadata, autoload_with=conn),
    )


def upgrade() -> None:
    conn = op.get_bind()
    TaskType, TaskTypeKey, TaskTypeKeyValue = get_tables(conn)

    task_type_key_id = conn.execute(
        TaskTypeKey.update()
        .returning(TaskTypeKey.c.task_type_key_id)
        .values(name=TASK_TYPE_KEY_NAME_NEW)
        .where(
            TaskTypeKey.c.name == TASK_TYPE_KEY_NAME_OLD,
            TaskTypeKey.c.task_type_id
            == sa.future.select(TaskType.c.task_type_id).where(TaskType.c.name == TASK_TYPE_NAME).scalar_subquery(),
        )
    ).scalar_one()

    conn.execute(
        TaskTypeKeyValue.update()
        .values(
            value=sa.cast(
                sa.func.json_build_object(TASK_TYPE_KEY_NAME_OLD, TaskTypeKeyValue.c.value),
                sa.String,
            )
        )
        .where(TaskTypeKeyValue.c.task_type_key_id == task_type_key_id)
    )


def downgrade() -> None:
    conn = op.get_bind()
    TaskType, TaskTypeKey, TaskTypeKeyValue = get_tables(conn)

    task_type_key_id = conn.execute(
        TaskTypeKey.update()
        .returning(TaskTypeKey.c.task_type_key_id)
        .values(name=TASK_TYPE_KEY_NAME_OLD)
        .where(
            TaskTypeKey.c.name == TASK_TYPE_KEY_NAME_NEW,
            TaskTypeKey.c.task_type_id
            == sa.future.select(TaskType.c.task_type_id).where(TaskType.c.name == TASK_TYPE_NAME).scalar_subquery(),
        )
    ).scalar_one()

    conn.execute(
        TaskTypeKeyValue.delete().where(
            TaskTypeKeyValue.c.task_type_key_id == task_type_key_id,
            ~TaskTypeKeyValue.c.value.contains(TASK_TYPE_KEY_NAME_OLD),
        )
    )

    conn.execute(
        TaskTypeKeyValue.update()
        .values(
            value=sa.func.json_extract_path_text(
                sa.func.cast(TaskTypeKeyValue.c.value, sa.JSON),
                TASK_TYPE_KEY_NAME_OLD,
            )
        )
        .where(
            TaskTypeKeyValue.c.task_type_key_id == task_type_key_id,
            TaskTypeKeyValue.c.value.contains(TASK_TYPE_KEY_NAME_OLD),
        )
    )
