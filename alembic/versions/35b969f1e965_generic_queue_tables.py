"""generic queue tables

Revision ID: 35b969f1e965
Revises: f21afdef225b
Create Date: 2021-09-30 17:48:14.878950

"""
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "35b969f1e965"
down_revision = "8b8f036e34ef"
branch_labels = None
depends_on = None


conn = op.get_bind()
taskparamskeytypes = postgresql.ENUM(
    "STRING", "INTEGER", "FLOAT", "BOOLEAN", "DATE", "DATETIME", name="taskparamskeytypes"
)
retrytaskstatuses = postgresql.ENUM(
    "PENDING", "IN_PROGRESS", "FAILED", "SUCCESS", "WAITING", "CANCELLED", name="retrytaskstatuses"
)


def populate_task_type_and_keys() -> None:
    metadata = sa.MetaData()
    task_type = sa.Table("task_type", metadata, autoload_with=conn)
    task_type_key = sa.Table("task_type_key", metadata, autoload_with=conn)

    populate_voucher_allocation_task_type_and_keys(task_type, task_type_key)
    populate_voucher_status_adjustment_task_type_and_keys(task_type, task_type_key)


def populate_voucher_allocation_task_type_and_keys(task_type: sa.Table, task_type_key: sa.Table) -> None:
    inserted_obj = conn.execute(
        sa.insert(task_type).values(name="voucher_issuance", path="app.tasks.allocation.issue_voucher")
    )
    task_type_id = inserted_obj.inserted_primary_key[0]
    key_data_list = [
        {"name": "account_url", "type": "STRING", "task_type_id": task_type_id},
        {"name": "issued_date", "type": "FLOAT", "task_type_id": task_type_id},
        {"name": "expiry_date", "type": "FLOAT", "task_type_id": task_type_id},
        {"name": "voucher_config_id", "type": "INTEGER", "task_type_id": task_type_id},
        {"name": "voucher_type_slug", "type": "STRING", "task_type_id": task_type_id},
        {"name": "voucher_id", "type": "STRING", "task_type_id": task_type_id},
        {"name": "voucher_code", "type": "STRING", "task_type_id": task_type_id},
    ]
    op.bulk_insert(task_type_key, key_data_list)


def populate_voucher_status_adjustment_task_type_and_keys(task_type: sa.Table, task_type_key: sa.Table) -> None:
    inserted_obj = conn.execute(
        sa.insert(task_type).values(
            name="voucher_status_adjustment", path="app.tasks.status_adjustment.status_adjustment"
        )
    )
    task_type_id = inserted_obj.inserted_primary_key[0]
    key_data_list = [
        {"name": "voucher_id", "type": "STRING", "task_type_id": task_type_id},
        {"name": "retailer_slug", "type": "STRING", "task_type_id": task_type_id},
        {"name": "date", "type": "FLOAT", "task_type_id": task_type_id},
        {"name": "status", "type": "STRING", "task_type_id": task_type_id},
    ]
    op.bulk_insert(task_type_key, key_data_list)


def upgrade() -> None:
    op.create_table(
        "task_type",
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("task_type_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False, index=True),
        sa.Column("path", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("task_type_id"),
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
        sa.Column("audit_data", postgresql.JSONB(astext_type=sa.Text()), nullable=False),  # type: ignore [call-arg]
        sa.Column("next_attempt_time", sa.DateTime(), nullable=True),
        sa.Column("status", retrytaskstatuses, nullable=False),
        sa.Column("task_type_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["task_type_id"], ["task_type.task_type_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("retry_task_id"),
    )
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
        sa.Column("type", taskparamskeytypes, nullable=False),
        sa.Column("task_type_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["task_type_id"], ["task_type.task_type_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("task_type_key_id"),
    )
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
    populate_task_type_and_keys()


def downgrade() -> None:
    op.drop_table("task_type_key_value")
    op.drop_table("task_type_key")
    op.drop_table("retry_task")
    op.drop_table("task_type")
    taskparamskeytypes.drop(conn, checkfirst=False)
    retrytaskstatuses.drop(conn, checkfirst=False)
