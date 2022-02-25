"""add fetch type path and create jigsaw

Revision ID: 629d5ac2463f
Revises: 16d946584f48
Create Date: 2022-02-18 17:50:49.449314

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "629d5ac2463f"
down_revision = "16d946584f48"
branch_labels = None
depends_on = None


def get_tables(conn: sa.engine.Connection) -> tuple[sa.Table, sa.Table, sa.Table, sa.Table, sa.Table]:
    metadata = sa.MetaData()
    return (
        sa.Table("fetch_type", metadata, autoload_with=conn),
        sa.Table("task_type", metadata, autoload_with=conn),
        sa.Table("task_type_key", metadata, autoload_with=conn),
        sa.Table("reward_config", metadata, autoload_with=conn),
        sa.Table("reward", metadata, autoload_with=conn),
    )


def upgrade() -> None:
    conn = op.get_bind()
    op.add_column("fetch_type", sa.Column("path", sa.String(), nullable=True))
    FetchType, TaskType, TaskTypeKey, _, _ = get_tables(conn)
    conn.execute(
        FetchType.update().values(path="app.fetch_reward.pre_loaded.PreLoaded").where(FetchType.c.name == "PRE_LOADED")
    )
    conn.execute(
        FetchType.insert().values(
            name="JIGSAW_EGIFT",
            path="app.fetch_reward.jigsaw.Jigsaw",
            required_fields="transaction_value: integer",
        )
    )
    conn.execute(
        sa.insert(TaskTypeKey).values(
            name="customer_card_ref",
            type="STRING",
            task_type_id=sa.future.select(TaskType.c.task_type_id)
            .where(TaskType.c.name == "reward-issuance")
            .scalar_subquery(),
        )
    )
    op.alter_column("fetch_type", "path", nullable=False)


def downgrade() -> None:
    op.drop_column("fetch_type", "path")
    conn = op.get_bind()
    FetchType, TaskType, TaskTypeKey, RewardConfig, Reward = get_tables(conn)
    conn.execute(
        Reward.delete().where(
            Reward.c.reward_config_id == RewardConfig.c.id,
            RewardConfig.c.fetch_type_id == FetchType.c.id,
            FetchType.c.name == "JIGSAW_EGIFT",
        )
    )
    conn.execute(FetchType.delete().where(FetchType.c.name == "JIGSAW_EGIFT"))
    conn.execute(
        sa.delete(TaskTypeKey).where(
            TaskTypeKey.c.name == "customer_card_ref",
            TaskTypeKey.c.task_type_id
            == sa.future.select(TaskType.c.task_type_id).where(TaskType.c.name == "reward-issuance").scalar_subquery(),
        )
    )
