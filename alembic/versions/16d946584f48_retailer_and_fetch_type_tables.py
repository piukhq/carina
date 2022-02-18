"""retailer and fetch type tables

Revision ID: 16d946584f48
Revises: 69cc1d0099e4
Create Date: 2022-02-16 17:17:35.431877

"""


import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "16d946584f48"
down_revision = "69cc1d0099e4"
branch_labels = None
depends_on = None

TASK_TO_UPDATE = "delete-unallocated-rewards"
REWARD_CONFIG_RETAILER_FK = "reward_config_retailer_id_fkey"
REWARD_RETAILER_FK = "reward_retailer_id_fkey"
REWARD_CONFIG_FETCH_TYPE_FK = "reward_config_fetch_type_id_fkey"

rewardfetchtype = sa.dialects.postgresql.ENUM("PRE_LOADED", name="rewardfetchtype")


def get_tables(
    conn: sa.engine.Connection,
) -> tuple[sa.Table, sa.Table, sa.Table, sa.Table, sa.Table, sa.Table, sa.Table]:
    metadata = sa.MetaData()
    return (
        sa.Table("retailer", metadata, autoload_with=conn),
        sa.Table("reward_config", metadata, autoload_with=conn),
        sa.Table("reward", metadata, autoload_with=conn),
        sa.Table("fetch_type", metadata, autoload_with=conn),
        sa.Table("task_type", metadata, autoload_with=conn),
        sa.Table("task_type_key", metadata, autoload_with=conn),
        sa.Table("task_type_key_value", metadata, autoload_with=conn),
    )


def update_table_slug_to_id(conn: sa.engine.Connection, table: sa.Table, slug_to_id_map: dict) -> None:
    conn.execute(table.update().values(retailer_id=sa.case(slug_to_id_map, value=table.c.retailer_slug)))


def update_table_id_to_slug(conn: sa.engine.Connection, table: sa.Table, id_to_slug_map: dict) -> None:
    conn.execute(table.update().values(retailer_slug=sa.case(id_to_slug_map, value=table.c.retailer_id)))


def upgrade() -> None:

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
        sa.ForeignKeyConstraint(
            ["fetch_type_id"],
            ["fetch_type.id"],
        ),
        sa.ForeignKeyConstraint(
            ["retailer_id"],
            ["retailer.id"],
        ),
        sa.PrimaryKeyConstraint("retailer_id", "fetch_type_id"),
    )
    op.add_column("reward", sa.Column("retailer_id", sa.Integer(), nullable=True))
    op.add_column("reward_config", sa.Column("retailer_id", sa.Integer(), nullable=True))
    op.add_column("reward_config", sa.Column("fetch_type_id", sa.Integer(), nullable=True))
    op.add_column("reward_config", sa.Column("required_fields_values", sa.Text(), nullable=True))

    # ------------------------------------------------ data migration ------------------------------------------------ #
    conn = op.get_bind()
    Retailer, RewardConfig, Reward, FetchType, TaskType, TaskTypeKey, TaskTypeKeyValue = get_tables(conn)

    fetch_type_id = conn.execute(
        FetchType.insert().values(name="PRE_LOADED", required_fields="validity_days: integer").returning(FetchType.c.id)
    ).scalar_one()

    if conn.scalar(sa.future.select(RewardConfig.c.id)):
        slug_to_id_map = dict(
            conn.execute(
                Retailer.insert().returning(Retailer.c.slug, Retailer.c.id),
                [
                    {"slug": res[0]}
                    for res in conn.execute(sa.future.select(RewardConfig.c.retailer_slug).distinct()).all()
                ],
            ).all()
        )

        conn.execute(RewardConfig.update().values(fetch_type_id=fetch_type_id))
        update_table_slug_to_id(conn, RewardConfig, slug_to_id_map)
        update_table_slug_to_id(conn, Reward, slug_to_id_map)
        conn.execute(
            RewardConfig.update().values(
                required_fields_values="validity_days: " + sa.cast(RewardConfig.c.validity_days, sa.String)
            )
        )

        task_type_key_id = conn.execute(
            TaskTypeKey.update()
            .where(
                TaskType.c.name == TASK_TO_UPDATE,
                TaskTypeKey.c.task_type_id == TaskType.c.task_type_id,
                TaskTypeKey.c.name == "retailer_slug",
            )
            .values(name="retailer_id", type="INTEGER")
            .returning(TaskTypeKey.c.task_type_key_id)
        ).scalar_one()
        conn.execute(
            TaskTypeKeyValue.update()
            .values(value=sa.case(slug_to_id_map, value=TaskTypeKeyValue.c.value))
            .where(TaskTypeKeyValue.c.task_type_key_id == task_type_key_id)
        )
    else:
        conn.execute(Reward.delete())

    # ---------------------------------------------------------------------------------------------------------------- #

    op.alter_column("reward", "retailer_id", nullable=False)
    op.alter_column("reward_config", "retailer_id", nullable=False)
    op.alter_column("reward_config", "fetch_type_id", nullable=False)

    op.drop_column("reward_config", "validity_days")
    op.create_foreign_key(REWARD_CONFIG_FETCH_TYPE_FK, "reward_config", "fetch_type", ["fetch_type_id"], ["id"])
    op.drop_column("reward_config", "fetch_type")
    op.drop_index("ix_reward_retailer_slug", table_name="reward")
    op.drop_constraint("reward_code_retailer_slug_reward_config_unq", "reward", type_="unique")
    op.create_unique_constraint(
        "code_retailer_reward_config_unq", "reward", ["code", "retailer_id", "reward_config_id"]
    )
    op.create_foreign_key(REWARD_RETAILER_FK, "reward", "retailer", ["retailer_id"], ["id"])
    op.drop_column("reward", "retailer_slug")

    op.drop_index("ix_reward_config_retailer_slug", table_name="reward_config")
    op.drop_constraint("reward_slug_retailer_slug_unq", "reward_config", type_="unique")
    op.create_unique_constraint("reward_slug_retailer_unq", "reward_config", ["reward_slug", "retailer_id"])
    op.create_foreign_key(REWARD_CONFIG_RETAILER_FK, "reward_config", "retailer", ["retailer_id"], ["id"])
    op.drop_column("reward_config", "retailer_slug")
    rewardfetchtype.drop(conn, checkfirst=False)


def downgrade() -> None:
    conn = op.get_bind()
    rewardfetchtype.create(conn, checkfirst=False)
    op.add_column(
        "reward_config", sa.Column("retailer_slug", sa.VARCHAR(length=32), autoincrement=False, nullable=True)
    )
    op.add_column("reward", sa.Column("retailer_slug", sa.VARCHAR(length=32), autoincrement=False, nullable=True))
    op.add_column("reward_config", sa.Column("fetch_type", rewardfetchtype, autoincrement=False, nullable=True))
    op.add_column("reward_config", sa.Column("validity_days", sa.INTEGER(), autoincrement=False, nullable=True))

    # ------------------------------------------------ data migration ------------------------------------------------ #
    Retailer, RewardConfig, Reward, _, TaskType, TaskTypeKey, TaskTypeKeyValue = get_tables(conn)

    conn.execute(RewardConfig.update().values(fetch_type="PRE_LOADED"))
    id_to_slug_map = dict(conn.execute(sa.future.select(Retailer.c.id, Retailer.c.slug)).all())
    update_table_id_to_slug(conn, RewardConfig, id_to_slug_map)
    update_table_id_to_slug(conn, Reward, id_to_slug_map)
    conn.execute(
        RewardConfig.update().values(
            validity_days=sa.case(
                [
                    (
                        RewardConfig.c.required_fields_values.like("validity_days: %"),
                        sa.cast(sa.func.substring(RewardConfig.c.required_fields_values, 16), sa.Integer),
                    ),
                ],
                else_=0,
            )
        )
    )

    task_type_key_id = conn.execute(
        TaskTypeKey.update()
        .where(
            TaskType.c.name == TASK_TO_UPDATE,
            TaskTypeKey.c.task_type_id == TaskType.c.task_type_id,
            TaskTypeKey.c.name == "retailer_id",
        )
        .values(name="retailer_slug", type="STRING")
        .returning(TaskTypeKey.c.task_type_key_id)
    ).scalar_one()
    conn.execute(
        TaskTypeKeyValue.update()
        .values(value=sa.case(id_to_slug_map, value=sa.cast(TaskTypeKeyValue.c.value, sa.Integer)))
        .where(TaskTypeKeyValue.c.task_type_key_id == task_type_key_id)
    )
    # ---------------------------------------------------------------------------------------------------------------- #

    op.alter_column("reward", "retailer_slug", nullable=False)
    op.alter_column("reward_config", "retailer_slug", nullable=False)
    op.alter_column("reward_config", "fetch_type", nullable=False)

    op.drop_column("reward_config", "required_fields_values")
    op.drop_constraint(REWARD_CONFIG_RETAILER_FK, "reward_config", type_="foreignkey")
    op.drop_constraint("reward_slug_retailer_unq", "reward_config", type_="unique")
    op.create_unique_constraint("reward_slug_retailer_slug_unq", "reward_config", ["reward_slug", "retailer_slug"])
    op.create_index("ix_reward_config_retailer_slug", "reward_config", ["retailer_slug"], unique=False)
    op.drop_column("reward_config", "retailer_id")

    op.drop_constraint(REWARD_CONFIG_FETCH_TYPE_FK, "reward_config", type_="foreignkey")
    op.drop_column("reward_config", "fetch_type_id")
    op.drop_constraint(REWARD_RETAILER_FK, "reward", type_="foreignkey")
    op.drop_constraint("code_retailer_reward_config_unq", "reward", type_="unique")
    op.create_unique_constraint(
        "reward_code_retailer_slug_reward_config_unq", "reward", ["code", "retailer_slug", "reward_config_id"]
    )
    op.create_index("ix_reward_retailer_slug", "reward", ["retailer_slug"], unique=False)
    op.drop_column("reward", "retailer_id")
    op.drop_table("retailer_fetch_type")
    op.drop_index(op.f("ix_retailer_slug"), table_name="retailer")
    op.drop_table("retailer")
    op.drop_table("fetch_type")
