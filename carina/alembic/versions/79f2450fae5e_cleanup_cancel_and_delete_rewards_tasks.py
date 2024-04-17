"""cleanup cancel and delete rewards tasks

Revision ID: 79f2450fae5e
Revises: 44b065b868a4
Create Date: 2022-10-20 12:32:24.122850

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "79f2450fae5e"
down_revision = "44b065b868a4"
branch_labels = None
depends_on = None

CANCEL_REWARDS_TASK_NAME = "cancel-rewards"
DELETE_UNALLOCATED_REWARDS_TASK_NAME = "delete-unallocated-rewards"


task_types = [
    {
        "name": CANCEL_REWARDS_TASK_NAME,
        "path": "carina.tasks.reward_cancellation.cancel_rewards",
        "error_handler_path": "carina.tasks.error_handlers.handle_retry_task_request_error",
    },
    {
        "name": DELETE_UNALLOCATED_REWARDS_TASK_NAME,
        "path": "carina.tasks.reward_deletion.delete_unallocated_rewards",
        "error_handler_path": "carina.tasks.error_handlers.default_handler",
    },
]

task_type_keys = {
    CANCEL_REWARDS_TASK_NAME: [
        {"name": "reward_slug", "type": "STRING"},
        {"name": "retailer_slug", "type": "STRING"},
    ],
    DELETE_UNALLOCATED_REWARDS_TASK_NAME: [
        {"name": "retailer_id", "type": "INTEGER"},
        {"name": "reward_slug", "type": "STRING"},
    ],
}


def upgrade() -> None:
    conn = op.get_bind()
    metadata = sa.MetaData()
    TaskType = sa.Table("task_type", metadata, autoload_with=conn)

    conn.execute(
        TaskType.delete().where(TaskType.c.name.in_([CANCEL_REWARDS_TASK_NAME, DELETE_UNALLOCATED_REWARDS_TASK_NAME]))
    )


def downgrade() -> None:
    conn = op.get_bind()
    metadata = sa.MetaData()
    TaskType = sa.Table("task_type", metadata, autoload_with=conn)
    TaskTypeKey = sa.Table("task_type_key", metadata, autoload_with=conn)

    inserted_task_types = conn.execute(
        TaskType.insert().returning(TaskType.c.name, TaskType.c.task_type_id).values(queue_name="carina:default"),
        task_types,
    ).all()

    for task_type_name, task_type_id in inserted_task_types:
        conn.execute(TaskTypeKey.insert().values(task_type_id=task_type_id), task_type_keys[task_type_name])
