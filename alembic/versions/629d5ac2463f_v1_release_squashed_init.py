"""v1 release squashed init

Revision ID: 629d5ac2463f
Revises: 
Create Date: 2022-03-05 11:27:13.470504

"""
from collections import namedtuple

import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "629d5ac2463f"
down_revision = None
branch_labels = None
depends_on = None

STRING = "STRING"
INTEGER = "INTEGER"
FLOAT = "FLOAT"

QUEUE_NAME = "carina:default"

TaskTypeKeyData = namedtuple("TaskTypeKeyData", ["name", "type"])
TaskTypeData = namedtuple("TaskTypeData", ["name", "path", "error_handler_path", "keys"])
task_type_data = [
    TaskTypeData(
        name="reward-issuance",
        path="carina.tasks.issuance.issue_reward",
        error_handler_path="carina.tasks.error_handlers.handle_issue_reward_request_error",
        keys=[
            TaskTypeKeyData(name="customer_card_ref", type=STRING),
            TaskTypeKeyData(name="idempotency_token", type=STRING),
            TaskTypeKeyData(name="expiry_date", type=FLOAT),
            TaskTypeKeyData(name="account_url", type=STRING),
            TaskTypeKeyData(name="reward_slug", type=STRING),
            TaskTypeKeyData(name="reward_uuid", type=STRING),
            TaskTypeKeyData(name="code", type=STRING),
            TaskTypeKeyData(name="reward_config_id", type=INTEGER),
            TaskTypeKeyData(name="issued_date", type=FLOAT),
        ],
    ),
    TaskTypeData(
        name="reward-status-adjustment",
        path="carina.tasks.status_adjustment.status_adjustment",
        error_handler_path="carina.tasks.error_handlers.handle_retry_task_request_error",
        keys=[
            TaskTypeKeyData(name="retailer_slug", type=STRING),
            TaskTypeKeyData(name="date", type=FLOAT),
            TaskTypeKeyData(name="status", type=STRING),
            TaskTypeKeyData(name="reward_uuid", type=STRING),
        ],
    ),
    TaskTypeData(
        name="delete-unallocated-rewards",
        path="carina.tasks.reward_deletion.delete_unallocated_rewards",
        error_handler_path="carina.tasks.error_handlers.default_handler",
        keys=[
            TaskTypeKeyData(name="reward_slug", type=STRING),
            TaskTypeKeyData(name="retailer_id", type=INTEGER),
        ],
    ),
    TaskTypeData(
        name="cancel-rewards",
        path="carina.tasks.reward_cancellation.cancel_rewards",
        error_handler_path="carina.tasks.error_handlers.handle_retry_task_request_error",
        keys=[
            TaskTypeKeyData(name="reward_slug", type=STRING),
            TaskTypeKeyData(name="retailer_slug", type=STRING),
        ],
    ),
]


def add_task_data(conn: sa.engine.Connection, metadata: sa.MetaData) -> None:
    TaskType = sa.Table("task_type", metadata, autoload_with=conn)
    TaskTypeKey = sa.Table("task_type_key", metadata, autoload_with=conn)
    for data in task_type_data:
        inserted_obj = conn.execute(
            TaskType.insert().values(
                name=data.name,
                path=data.path,
                error_handler_path=data.error_handler_path,
                queue_name=QUEUE_NAME,
            )
        )
        task_type_id = inserted_obj.inserted_primary_key[0]
        for key in data.keys:
            conn.execute(TaskTypeKey.insert().values(name=key.name, type=key.type, task_type_id=task_type_id))


def add_fetch_types(conn: sa.engine.Connection, metadata: sa.MetaData) -> None:
    FetchType = sa.Table("fetch_type", metadata, autoload_with=conn)
    conn.execute(
        FetchType.insert(),
        [
            {
                "name": "PRE_LOADED",
                "required_fields": "validity_days: integer",
                "path": "carina.fetch_reward.pre_loaded.PreLoaded",
            },
            {
                "name": "JIGSAW_EGIFT",
                "required_fields": "transaction_value: integer",
                "path": "carina.fetch_reward.jigsaw.Jigsaw",
            },
        ],
    )


def upgrade() -> None:
    metadata = sa.MetaData()
    conn = op.get_bind()
    op.create_table(
        "fetch_type",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("required_fields", sa.Text(), nullable=True),
        sa.Column("path", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "retailer",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("slug", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_retailer_slug"), "retailer", ["slug"], unique=False)
    op.create_table(
        "reward_file_log",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("file_name", sa.String(length=500), nullable=False),
        sa.Column("file_agent_type", sa.Enum("IMPORT", "UPDATE", name="fileagenttype"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("file_name", "file_agent_type", name="file_name_file_agent_type_unq"),
    )
    op.create_index(op.f("ix_reward_file_log_file_agent_type"), "reward_file_log", ["file_agent_type"], unique=False)
    op.create_index(op.f("ix_reward_file_log_file_name"), "reward_file_log", ["file_name"], unique=False)
    op.create_table(
        "task_type",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("task_type_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("error_handler_path", sa.String(), nullable=False),
        sa.Column("queue_name", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("task_type_id"),
    )
    op.create_index(op.f("ix_task_type_name"), "task_type", ["name"], unique=True)
    op.create_table(
        "retailer_fetch_type",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("retailer_id", sa.Integer(), nullable=False),
        sa.Column("fetch_type_id", sa.Integer(), nullable=False),
        sa.Column("agent_config", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["fetch_type_id"], ["fetch_type.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["retailer_id"], ["retailer.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("retailer_id", "fetch_type_id"),
    )
    op.create_table(
        "retry_task",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("retry_task_id", sa.Integer(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("audit_data", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("next_attempt_time", sa.DateTime(), nullable=True),
        sa.Column(
            "status",
            sa.Enum(
                "PENDING",
                "IN_PROGRESS",
                "RETRYING",
                "FAILED",
                "SUCCESS",
                "WAITING",
                "CANCELLED",
                "REQUEUED",
                name="retrytaskstatuses",
            ),
            nullable=False,
        ),
        sa.Column("task_type_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["task_type_id"], ["task_type.task_type_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("retry_task_id"),
    )
    op.create_table(
        "reward_config",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("reward_slug", sa.String(length=32), nullable=False),
        sa.Column("retailer_id", sa.Integer(), nullable=False),
        sa.Column("fetch_type_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.Enum("ACTIVE", "CANCELLED", "ENDED", name="rewardtypestatuses"), nullable=False),
        sa.Column("required_fields_values", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["fetch_type_id"], ["fetch_type.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["retailer_id"], ["retailer.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("reward_slug", "retailer_id", name="reward_slug_retailer_unq"),
    )
    op.create_index(op.f("ix_reward_config_reward_slug"), "reward_config", ["reward_slug"], unique=False)
    op.create_table(
        "task_type_key",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("task_type_key_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column(
            "type",
            sa.Enum("STRING", "INTEGER", "FLOAT", "BOOLEAN", "DATE", "DATETIME", name="taskparamskeytypes"),
            nullable=False,
        ),
        sa.Column("task_type_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["task_type_id"], ["task_type.task_type_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("task_type_key_id"),
        sa.UniqueConstraint("name", "task_type_id", name="name_task_type_id_unq"),
    )
    op.create_table(
        "reward",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("code", sa.String(), nullable=False),
        sa.Column("allocated", sa.Boolean(), nullable=False),
        sa.Column("deleted", sa.Boolean(), nullable=False),
        sa.Column("reward_config_id", sa.Integer(), nullable=False),
        sa.Column("retailer_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["retailer_id"], ["retailer.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["reward_config_id"],
            ["reward_config.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code", "retailer_id", "reward_config_id", name="code_retailer_reward_config_unq"),
    )
    op.create_index(op.f("ix_reward_code"), "reward", ["code"], unique=False)
    op.create_table(
        "task_type_key_value",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("value", sa.String(), nullable=True),
        sa.Column("retry_task_id", sa.Integer(), nullable=False),
        sa.Column("task_type_key_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["retry_task_id"], ["retry_task.retry_task_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["task_type_key_id"], ["task_type_key.task_type_key_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("retry_task_id", "task_type_key_id"),
    )
    op.create_table(
        "reward_update",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("reward_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("status", sa.Enum("ISSUED", "CANCELLED", "REDEEMED", name="rewardupdatestatuses"), nullable=False),
        sa.ForeignKeyConstraint(["reward_uuid"], ["reward.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    add_task_data(conn, metadata)
    add_fetch_types(conn, metadata)


def downgrade() -> None:
    op.drop_table("reward_update")
    op.drop_table("task_type_key_value")
    op.drop_index(op.f("ix_reward_code"), table_name="reward")
    op.drop_table("reward")
    op.drop_table("task_type_key")
    op.drop_index(op.f("ix_reward_config_reward_slug"), table_name="reward_config")
    op.drop_table("reward_config")
    op.drop_table("retry_task")
    op.drop_table("retailer_fetch_type")
    op.drop_index(op.f("ix_task_type_name"), table_name="task_type")
    op.drop_table("task_type")
    op.drop_index(op.f("ix_reward_file_log_file_name"), table_name="reward_file_log")
    op.drop_index(op.f("ix_reward_file_log_file_agent_type"), table_name="reward_file_log")
    op.drop_table("reward_file_log")
    op.drop_index(op.f("ix_retailer_slug"), table_name="retailer")
    op.drop_table("retailer")
    op.drop_table("fetch_type")
