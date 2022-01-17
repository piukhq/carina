"""rename voucher_type_slug to reward_slug

Revision ID: 64dadbdcd039
Revises: 9589c7115a36
Create Date: 2022-01-13 16:55:42.361958

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "64dadbdcd039"
down_revision = "9589c7115a36"
branch_labels = None
depends_on = None


def upgrade():
    # Update task_type.name to cancel-rewards
    op.execute(
        "UPDATE task_type SET path = 'app.tasks.reward_cancellation.cancel_rewards', name = 'cancel-rewards' "
        "WHERE name ='cancel-vouchers' AND path = 'app.tasks.voucher_cancellation.cancel_vouchers'"
    )

    # Update task_type_key.name for voucher_type_slug
    op.execute("UPDATE task_type_key SET name = 'reward_slug' from task_type where task_type.name = 'cancel-rewards' and task_type_key.task_type_id = task_type.task_type_id")

    # Update task_type.name to delete-unallocated-rewards
    op.execute(
        "UPDATE task_type SET path = 'app.tasks.reward_deletion.delete_unallocated_rewards', name = 'delete-unallocated-rewards' "
        "WHERE name = 'delete-unallocated-vouchers' AND path = 'app.tasks.voucher_deletion.delete_unallocated_vouchers'"
    )

    # Update task_type_key.name for voucher_type_slug
    op.execute("UPDATE task_type_key SET name = 'reward_slug' from task_type where task_type.name = 'delete-unallocated-rewards' and task_type_key.task_type_id = task_type.task_type_id")




def downgrade():
    # Revert task_type.name to cancel-vouchers
    op.execute(
        "UPDATE task_type SET path = 'app.tasks.voucher_cancellation.cancel_vouchers', name = 'cancel-vouchers' "
        "WHERE name ='cancel-rewards' AND path = 'app.tasks.reward_cancellation.cancel_rewards'"
    )

    # Revert task_type_key.name for reward_slug
    op.execute("UPDATE task_type_key SET name = 'voucher_type_slug' from task_type where task_type.name = 'cancel-vouchers' and task_type_key.task_type_id = task_type.task_type_id")


    # Revert task_type.name to delete-unallocated-vouchers
    op.execute(
        "UPDATE task_type SET path = 'app.tasks.voucher_deletion.delete_unallocated_vouchers', name = 'delete-unallocated-vouchers' "
        "WHERE name ='delete-unallocated-rewards' AND path = 'app.tasks.reward_deletion.delete_unallocated_rewards'"
    )

    # Revert task_type_key.name for reward_slug
    op.execute("UPDATE task_type_key SET name = 'voucher_type_slug' from task_type where task_type.name = 'delete-unallocated-rewards' and task_type_key.task_type_id = task_type.task_type_id")

