"""add task type key

Revision ID: 4f5e04911e0a
Revises: be271e924073
Create Date: 2021-11-10 13:18:14.879283

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "4f5e04911e0a"
down_revision = "be271e924073"
branch_labels = None
depends_on = None

new_task_type_key_name = "idempotency_token"
task_type_name = "voucher-issuance"


def upgrade() -> None:
    op.get_bind().execute(
        sa.text(
            """
            INSERT INTO task_type_key (task_type_id, name, type)
            VALUES (
                (SELECT task_type.task_type_id FROM task_type WHERE task_type.name = :task_name),
                :key_name,
                'STRING'
            )
            """
        ),
        key_name=new_task_type_key_name,
        task_name=task_type_name,
    )


def downgrade() -> None:
    op.get_bind().execute(
        sa.text(
            """
            DELETE FROM task_type_key 
            WHERE task_type_key.task_type_id = (
                SELECT task_type.task_type_id FROM task_type WHERE task_type.name = :task_name
            )
            AND task_type_key.name = :key_name
            """
        ),
        key_name=new_task_type_key_name,
        task_name=task_type_name,
    )
