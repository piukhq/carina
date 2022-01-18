"""rename delete_voucher task to delete_rewards task

Revision ID: af1ac6c1c854
Revises: 64dadbdcd039
Create Date: 2022-01-17 13:25:55.087799

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "af1ac6c1c854"
down_revision = "64dadbdcd039"
branch_labels = None
depends_on = None


def upgrade():
    # Update task_type.name to delete-unallocated-rewards
    op.execute(
        "UPDATE task_type SET path = 'app.tasks.reward_deletion.delete_unallocated_rewards', name = 'delete-unallocated-rewards' "
        "WHERE name = 'delete-unallocated-vouchers' AND path = 'app.tasks.voucher_deletion.delete_unallocated_vouchers'"
    )

    # Update task_type_key.name for voucher_type_slug
    op.execute(
        "UPDATE task_type_key SET name = 'reward_slug' from task_type where task_type.name = 'delete-unallocated-rewards' and task_type_key.task_type_id = task_type.task_type_id and task_type_key.name = 'voucher_type_slug'"
    )


def downgrade():
    # Revert task_type.name to delete-unallocated-vouchers
    op.execute(
        "UPDATE task_type SET path = 'app.tasks.voucher_deletion.delete_unallocated_vouchers', name = 'delete-unallocated-vouchers' "
        "WHERE name ='delete-unallocated-rewards' AND path = 'app.tasks.reward_deletion.delete_unallocated_rewards'"
    )

    # # Revert task_type_key.name for reward_slug
    op.execute(
        "UPDATE task_type_key SET name = 'voucher_type_slug' from task_type where task_type.name = 'delete-unallocated-rewards' and task_type_key.task_type_id = task_type.task_type_id and task_type_key.name = 'reward_slug'"
    )
