"""rename voucher tables to reward

Revision ID: 69cc1d0099e4
Revises: af1ac6c1c854
Create Date: 2022-01-20 11:34:29.079828

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "69cc1d0099e4"
down_revision = "68cf212e6eeb"
branch_labels = None
depends_on = None


task_type_name = "reward-issuance"
changes = [
    ("voucher_config_id", "reward_config_id"),
    ("voucher_type_slug", "reward_slug"),
    ("voucher_id", "reward_uuid"),
    ("voucher_code", "code"),
]


def update_naming(current, new_name):
    op.execute(
        f"UPDATE task_type_key SET name = '{new_name}' FROM task_type WHERE task_type.name = '{task_type_name}' AND task_type_key.task_type_id = task_type.task_type_id AND task_type_key.name = '{current}'"
    )


def upgrade():
    conn = op.get_bind()
    existing_metadata = sa.schema.MetaData()

    ## 1st table - Voucher

    new_reward_table = "reward"
    old_voucher_table = "voucher"

    # Drop indices
    for index in ("ix_voucher_retailer_slug", "ix_voucher_voucher_code"):
        op.drop_index(op.f(index), table_name=old_voucher_table)

    op.drop_constraint("voucher_code_retailer_slug_voucher_config_unq", table_name=old_voucher_table)

    # rename tables
    op.rename_table(old_voucher_table, new_reward_table)

    # Drop referenced FK from voucher_update table
    op.drop_constraint("voucher_update_voucher_id_fkey", "voucher_update")  # voucher_update table

    # Drop PK and FK from existing table
    existing_table = sa.Table(new_reward_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_reward_table)
    op.drop_constraint("voucher_voucher_config_id_fkey", new_reward_table)

    # Alter columns
    op.alter_column(table_name=new_reward_table, column_name="voucher_code", new_column_name="code")
    op.alter_column(table_name=new_reward_table, column_name="voucher_config_id", new_column_name="reward_config_id")

    # Re-create FK and PK
    # FK created during 2nd table migration due to dependency, L: 80
    op.create_primary_key(f"{new_reward_table}_pkey", new_reward_table, ["id"])

    # Re-create indices
    op.create_unique_constraint(
        "reward_code_retailer_slug_reward_config_unq", new_reward_table, ["code", "retailer_slug", "reward_config_id"]
    )

    op.create_index("ix_reward_retailer_slug", new_reward_table, ["retailer_slug"])
    op.create_index("ix_reward_code", new_reward_table, ["code"])

    ## 2nd table - VoucherConfig

    old_voucher_config_table = "voucher_config"
    new_reward_config_table = "reward_config"

    # renamed voucher_config -> reward_config
    op.rename_table("voucher_config", "reward_config")

    # Drop indices
    for index in ("ix_voucher_config_retailer_slug", "ix_voucher_config_voucher_type_slug"):
        op.drop_index(op.f(index), table_name=old_voucher_config_table)

    op.drop_constraint("voucher_type_slug_retailer_slug_unq", table_name=new_reward_config_table)

    # Drop PK from existing table
    existing_table = sa.Table(new_reward_config_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_reward_config_table)

    # Re-create PK
    op.create_primary_key(f"{new_reward_config_table}_pkey", new_reward_config_table, ["id"])
    op.create_foreign_key(
        f"{new_reward_table}_reward_config_id_fkey",
        new_reward_table,
        new_reward_config_table,
        ["reward_config_id"],
        ["id"],
    )

    # Alter sequence
    op.execute(f"ALTER SEQUENCE {old_voucher_config_table}_id_seq RENAME TO {new_reward_config_table}_id_seq;")

    # Alter enums
    op.execute("ALTER TYPE voucherfetchtype RENAME TO rewardfetchtype")

    # Alter columns
    op.alter_column(table_name=new_reward_config_table, column_name="voucher_type_slug", new_column_name="reward_slug")

    # Re-create indices
    op.create_unique_constraint(
        "reward_slug_retailer_slug_unq", new_reward_config_table, ["reward_slug", "retailer_slug"]
    )

    op.create_index("ix_reward_config_retailer_slug", new_reward_config_table, ["retailer_slug"])
    op.create_index("ix_reward_config_reward_slug", new_reward_config_table, ["reward_slug"])

    ## 3rd table - VoucherUpdate

    old_voucher_update_table = "voucher_update"
    new_reward_update_table = "reward_update"

    # rename voucher_config table -> reward_config
    op.rename_table(old_voucher_update_table, new_reward_update_table)

    # Drop PK and FK from existing table
    existing_table = sa.Table(new_reward_update_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_reward_update_table)
    # FK dropped earlier due to dependency in voucher table, L: 38

    # Alter columns
    op.alter_column(table_name=new_reward_update_table, column_name="voucher_id", new_column_name="reward_uuid")

    # Re-create FK and PK
    referent_reward_table = "reward"
    op.create_foreign_key(
        f"{new_reward_update_table}_reward_uuid_fkey",
        new_reward_update_table,
        referent_reward_table,
        ["reward_uuid"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_primary_key(f"{new_reward_update_table}_pkey", new_reward_update_table, ["id"])

    # Alter sequence
    op.execute(f"ALTER SEQUENCE {old_voucher_update_table}_id_seq RENAME TO {new_reward_update_table}_id_seq;")

    ## 4th table - VoucherFileLog

    old_voucher_file_log_table = "voucher_file_log"
    new_reward_file_log_table = "reward_file_log"

    # Drop indices
    for index in (f"ix_{old_voucher_file_log_table}_file_name", f"ix_{old_voucher_file_log_table}_file_agent_type"):
        op.drop_index(op.f(index), table_name=old_voucher_file_log_table)

    # rename voucher_file_log table -> reward_file_log
    op.rename_table(old_voucher_file_log_table, new_reward_file_log_table)

    # Drop PK from existing table
    existing_table = sa.Table(new_reward_file_log_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_reward_file_log_table)

    # Re-create PK
    op.create_primary_key(f"{new_reward_file_log_table}_pkey", new_reward_file_log_table, ["id"])

    # Re-create indices
    op.create_index(f"ix_{new_reward_file_log_table}_file_name", new_reward_file_log_table, ["file_name"])
    op.create_index(f"ix_{new_reward_file_log_table}_file_agent_type", new_reward_file_log_table, ["file_agent_type"])

    # Alter sequence
    op.execute(f"ALTER SEQUENCE {old_voucher_file_log_table}_id_seq RENAME TO {new_reward_file_log_table}_id_seq;")

    # Update reward-issuance task fields, this depends on the task params being changed so putting it here
    for old_key, new_key in changes:
        update_naming(old_key, new_key)

    # Update reward-status-adjustment field
    op.execute(
        "UPDATE task_type_key SET name = 'reward_uuid' from task_type where task_type.name = 'reward-status-adjustment' and task_type_key.task_type_id = task_type.task_type_id and task_type_key.name = 'voucher_id'"
    )

    # Update error handler path
    op.execute(
        "UPDATE task_type SET error_handler_path = 'app.tasks.error_handlers.handle_issue_reward_request_error' WHERE error_handler_path = 'app.tasks.error_handlers.handle_issue_voucher_request_error'"
    )


def downgrade():
    conn = op.get_bind()
    existing_metadata = sa.schema.MetaData()

    # VoucherTable

    new_voucher_table = "voucher"
    old_reward_table = "reward"

    # Drop indices
    for index in ("ix_reward_retailer_slug", "ix_reward_code"):
        op.drop_index(op.f(index), table_name=old_reward_table)

    op.drop_constraint("reward_code_retailer_slug_reward_config_unq", table_name=old_reward_table)

    # rename tables
    op.rename_table(old_reward_table, new_voucher_table)

    # Drop referenced FK from voucher_update table
    op.drop_constraint("reward_update_reward_uuid_fkey", "reward_update")  # voucher_update table

    # Drop PK and FK from existing table
    existing_table = sa.Table(new_voucher_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_voucher_table)
    op.drop_constraint("reward_reward_config_id_fkey", new_voucher_table)

    # Alter columns
    op.alter_column(table_name=new_voucher_table, column_name="code", new_column_name="voucher_code")
    op.alter_column(table_name=new_voucher_table, column_name="reward_config_id", new_column_name="voucher_config_id")

    # Re-create FK and PK
    # FK created during 2nd table migration due to dependency, L: 218
    op.create_primary_key(f"{new_voucher_table}_pkey", new_voucher_table, ["id"])

    # Re-create indices
    op.create_unique_constraint(
        "voucher_code_retailer_slug_voucher_config_unq",
        new_voucher_table,
        ["voucher_code", "retailer_slug", "voucher_config_id"],
    )

    op.create_index("ix_voucher_retailer_slug", new_voucher_table, ["retailer_slug"])
    op.create_index("ix_voucher_voucher_code", new_voucher_table, ["voucher_code"])

    # VoucherConfig

    old_reward_config_table = "reward_config"
    new_voucher_config_table = "voucher_config"

    # rename voucher_config table -> reward_config
    op.rename_table(old_reward_config_table, new_voucher_config_table)

    # Drop indices
    for index in ("ix_reward_config_retailer_slug", "ix_reward_config_reward_slug"):
        op.drop_index(op.f(index), table_name=old_reward_config_table)

    op.drop_constraint("reward_slug_retailer_slug_unq", table_name=new_voucher_config_table)

    # Drop PK from existing table
    existing_table = sa.Table(new_voucher_config_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_voucher_config_table)

    # Re-create PK
    referent_table = "voucher_config"
    op.create_primary_key(f"{new_voucher_config_table}_pkey", new_voucher_config_table, ["id"])
    op.create_foreign_key(
        f"{new_voucher_table}_voucher_config_id_fkey", new_voucher_table, referent_table, ["voucher_config_id"], ["id"]
    )

    # Alter sequence
    op.execute(f"ALTER SEQUENCE {old_reward_config_table}_id_seq RENAME TO {new_voucher_config_table}_id_seq;")

    # Alter enums
    op.execute("ALTER TYPE rewardfetchtype RENAME TO voucherfetchtype")

    # Alter columns
    op.alter_column(table_name=new_voucher_config_table, column_name="reward_slug", new_column_name="voucher_type_slug")

    # Re-create indices
    op.create_unique_constraint(
        "voucher_type_slug_retailer_slug_unq", new_voucher_config_table, ["voucher_type_slug", "retailer_slug"]
    )

    op.create_index("ix_voucher_config_retailer_slug", new_voucher_config_table, ["retailer_slug"])
    op.create_index("ix_voucher_config_voucher_type_slug", new_voucher_config_table, ["voucher_type_slug"])

    # VoucherUpdate

    old_reward_update_table = "reward_update"
    new_voucher_update_table = "voucher_update"

    # rename voucher_config table -> reward_config
    op.rename_table(old_reward_update_table, new_voucher_update_table)

    # Drop PK and FK from existing table
    existing_table = sa.Table(new_voucher_update_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_voucher_update_table)
    # FK dropped earlier due to dependency in reward table, L: 172

    # Alter columns
    op.alter_column(table_name=new_voucher_update_table, column_name="reward_uuid", new_column_name="voucher_id")

    # Re-create FK and PK
    referent_reward_table = "voucher"  ##TODO: This depends on voucher table rename
    op.create_foreign_key(
        f"{new_voucher_update_table}_voucher_id_fkey",
        new_voucher_update_table,
        referent_reward_table,
        ["voucher_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_primary_key(f"{new_voucher_update_table}_pkey", new_voucher_update_table, ["id"])

    # Alter sequence
    op.execute(f"ALTER SEQUENCE {old_reward_update_table}_id_seq RENAME TO {new_voucher_update_table}_id_seq;")

    # VoucherFileLog

    old_reward_file_log_table = "reward_file_log"
    new_voucher_file_log_table = "voucher_file_log"

    # Drop indices
    for index in (f"ix_{old_reward_file_log_table}_file_name", f"ix_{old_reward_file_log_table}_file_agent_type"):
        op.drop_index(op.f(index), table_name=old_reward_file_log_table)

    # rename voucher_file_log table -> reward_file_log
    op.rename_table(old_reward_file_log_table, new_voucher_file_log_table)

    # Drop PK from existing table
    existing_table = sa.Table(new_voucher_file_log_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_voucher_file_log_table)

    # Re-create PK
    op.create_primary_key(f"{new_voucher_file_log_table}_pkey", new_voucher_file_log_table, ["id"])

    # Re-create indices
    op.create_index(f"ix_{new_voucher_file_log_table}_file_name", new_voucher_file_log_table, ["file_name"])
    op.create_index(f"ix_{new_voucher_file_log_table}_file_agent_type", new_voucher_file_log_table, ["file_agent_type"])

    # Alter sequence
    op.execute(f"ALTER SEQUENCE {old_reward_file_log_table}_id_seq RENAME TO {new_voucher_file_log_table}_id_seq;")

    # Update voucher-issuance task fields, this depends on the task params being changed so putting it here
    for old_key, new_key in changes:
        update_naming(new_key, old_key)

    # Update voucher-status-adjustment field
    op.execute(
        "UPDATE task_type_key SET name = 'voucher_id' from task_type where task_type.name = 'reward-status-adjustment' and task_type_key.task_type_id = task_type.task_type_id and task_type_key.name = 'reward_uuid'"
    )

    # Update error handler path
    op.execute(
        "UPDATE task_type SET error_handler_path = 'app.tasks.error_handlers.handle_issue_voucher_request_error' WHERE error_handler_path = 'app.tasks.error_handlers.handle_issue_reward_request_error'"
    )
