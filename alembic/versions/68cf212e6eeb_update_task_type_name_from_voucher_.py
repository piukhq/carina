"""Update task_type voucher-issuance and issue_voucher task name

Revision ID: 68cf212e6eeb
Revises: af1ac6c1c854
Create Date: 2022-01-21 14:16:33.329657

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "68cf212e6eeb"
down_revision = "ddf43b63fcb0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE task_type SET path = 'app.tasks.issuance.issue_reward', name = 'reward-issuance' "
        "WHERE name ='voucher-issuance' AND path = 'app.tasks.issuance.issue_voucher'"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE task_type SET path = 'app.tasks.issuance.issue_voucher', name = 'voucher-issuance' "
        "WHERE name ='reward-issuance' AND path = 'app.tasks.issuance.issue_reward'"
    )
