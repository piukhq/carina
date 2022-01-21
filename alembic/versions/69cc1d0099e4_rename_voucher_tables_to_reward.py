"""rename voucher tables to reward

Revision ID: 69cc1d0099e4
Revises: af1ac6c1c854
Create Date: 2022-01-20 11:34:29.079828

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "69cc1d0099e4"
down_revision = "af1ac6c1c854"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    existing_metadata = sa.schema.MetaData()

    ### 1st table - Voucher
    # Need to rename columns:
    # id: Need to drop PK
    # voucher_code -> code
    # voucher_config_id -> reward_config_id, need to drop FK 
    # reward_config relationship
    # Renaming constraints

    new_reward_table = "reward"
    old_voucher_table = "voucher"

    # Drop indices
    for index in ("ix_voucher_retailer_slug", "ix_voucher_voucher_code"):
        op.drop_index(op.f(index), table_name=old_voucher_table)

    op.drop_constraint("voucher_code_retailer_slug_voucher_config_unq", table_name=old_voucher_table)

    # rename tables
    op.rename_table(old_voucher_table, new_reward_table)
    op.rename_table("voucher_config", "reward_config")

    # Drop referenced FK from voucher_update table
    op.drop_constraint("voucher_update_voucher_id_fkey", "voucher_update") # voucher_update table

    # Drop PK and FK from existing table
    existing_table = sa.Table(new_reward_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_reward_table)
    op.drop_constraint("voucher_voucher_config_id_fkey", new_reward_table)

    # Alter columns
    op.alter_column(table_name=new_reward_table, column_name="voucher_code", new_column_name="code")    
    op.alter_column(table_name=new_reward_table, column_name="voucher_config_id", new_column_name="reward_config_id")

    # Re-create FK and PK
    # referent_table = "reward_config" ##TODO: This depends on voucher_config table rename
    # op.create_foreign_key(f"{new_reward_table}_reward_config_id_fkey", "reward", "reward_config", ["reward_config_id"], ["id"])
    op.create_primary_key(f"{new_reward_table}_pkey", new_reward_table, ["id"])

    # Re-create indices
    op.create_unique_constraint("reward_code_retailer_slug_reward_config_unq", new_reward_table, ["code", "retailer_slug", "reward_config_id"])
    
    op.create_index("ix_reward_retailer_slug", new_reward_table, ["retailer_slug"])
    op.create_index("ix_reward_code", new_reward_table, ["code"])


    ### 2nd table - VoucherConfig
    # Need to rename columns:
    # id: Need to drop PK
    # voucher_type_slug -> reward_slug
    # Fetch type and status has enums which are renamed
    # rewards relationship
    # Renaming constraints

    old_voucher_config_table = "voucher_config"
    new_reward_config_table = "reward_config"

    # Drop indices
    for index in ("ix_voucher_config_retailer_slug", "ix_voucher_config_voucher_type_slug"):
        op.drop_index(op.f(index), table_name=old_voucher_config_table)

    op.drop_constraint("voucher_type_slug_retailer_slug_unq", table_name=new_reward_config_table)
    
    # rename voucher_config table -> reward_config
    # op.rename_table(old_voucher_config_table, new_reward_config_table) Already done in 1st table

    # Drop PK from existing table
    existing_table = sa.Table(new_reward_config_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_reward_config_table)

    # Re-create PK
    op.create_primary_key(f"{new_reward_config_table}_pkey", new_reward_config_table, ["id"])
    op.create_foreign_key(f"{new_reward_table}_reward_config_id_fkey", "reward", "reward_config", ["reward_config_id"], ["id"])
  
    # Alter sequence
    op.execute(f"ALTER SEQUENCE {old_voucher_config_table}_id_seq RENAME TO {new_reward_config_table}_id_seq;")

    # Alter enums
    op.execute("ALTER TYPE voucherfetchtype RENAME TO rewardfetchtype")
    
    # Alter columns
    op.alter_column(table_name=new_reward_config_table, column_name="voucher_type_slug", new_column_name="reward_slug")    

    # Re-create indices
    op.create_unique_constraint("reward_slug_retailer_slug_unq", new_reward_config_table, ["reward_slug", "retailer_slug"])
    
    op.create_index("ix_reward_config_retailer_slug", new_reward_config_table, ["retailer_slug"])
    op.create_index("ix_reward_config_reward_slug", new_reward_config_table, ["reward_slug"])

    
    # 3rd table - VoucherUpdate

    # Need to rename columns:
    # id: Need to drop PK
    # voucher_id -> reward_uuid, need to drop FK and rename
    # reward relationship
    # No unique constraints

    old_voucher_update_table = "voucher_update"
    new_reward_update_table = "reward_update"
    
    # rename voucher_config table -> reward_config
    op.rename_table(old_voucher_update_table, new_reward_update_table)

    # Drop PK from existing table
    existing_table = sa.Table(new_reward_update_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_reward_update_table)
    # op.drop_constraint(existing_table.foreign_key_constraints, new_reward_update_table)

    # Alter columns
    op.alter_column(table_name=new_reward_update_table, column_name="voucher_id", new_column_name="reward_uuid")    

    # Re-create FK and PK
    referent_reward_table = "reward" ##TODO: This depends on voucher table rename
    op.create_foreign_key(f"{new_reward_update_table}_reward_uuid_fkey", new_reward_update_table, referent_reward_table, ["reward_uuid"], ["id"], ondelete="CASCADE",)
    op.create_primary_key(f"{new_reward_update_table}_pkey", new_reward_update_table, ["id"])

    # Alter sequence
    op.execute(f"ALTER SEQUENCE {old_voucher_update_table}_id_seq RENAME TO {new_reward_update_table}_id_seq;")
    


    ## 4th table - VoucherFileLog

    # id: Need to drop PK
    # unique constraints rename

    old_voucher_file_log_table = "voucher_file_log"
    new_reward_file_log_table = "reward_file_log"

    # Drop indices
    for index in (f"ix_{old_voucher_file_log_table}_file_name", f"ix_{old_voucher_file_log_table}_file_agent_type"):
        op.drop_index(op.f(index), table_name=old_voucher_file_log_table)
    # op.drop_constraint("file_name_file_agent_type_unq", table_name=old_voucher_file_log_table)

    # rename voucher_file_log table -> reward_file_log
    op.rename_table(old_voucher_file_log_table, new_reward_file_log_table)

    # Drop PK from existing table
    existing_table = sa.Table(new_reward_file_log_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_reward_file_log_table)

    # Re-create PK
    op.create_primary_key(f"{new_reward_file_log_table}_pkey", new_reward_file_log_table, ["id"])

    # Re-create indices
    # op.create_unique_constraint("file_name_file_agent_type_unq", new_reward_file_log_table, ["file_name", "file_agent_type"])
    op.create_index(f"ix_{new_reward_file_log_table}_file_name", new_reward_file_log_table, ["file_name"])
    op.create_index(f"ix_{new_reward_file_log_table}_file_agent_type", new_reward_file_log_table, ["file_agent_type"])

    # Alter sequence
    op.execute(f"ALTER SEQUENCE {old_voucher_file_log_table}_id_seq RENAME TO {new_reward_file_log_table}_id_seq;")

    


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
    op.rename_table("reward_config", "voucher_config")

    # Drop referenced FK from voucher_update table
    op.drop_constraint("reward_update_reward_uuid_fkey", "reward_update") # voucher_update table

    # Drop PK and FK from existing table
    existing_table = sa.Table(new_voucher_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_voucher_table)
    op.drop_constraint("reward_reward_config_id_fkey", new_voucher_table)

    # Alter columns
    op.alter_column(table_name=new_voucher_table, column_name="code", new_column_name="voucher_code")    
    op.alter_column(table_name=new_voucher_table, column_name="reward_config_id", new_column_name="voucher_config_id")

    # Re-create FK and PK
    referent_table = "voucher_config" ##TODO: This depends on voucher_config table rename
    # op.create_foreign_key(f"{new_voucher_table}_voucher_config_id_fkey", new_voucher_table, referent_table, ["voucher_config_id"], ["id"])
    op.create_primary_key(f"{new_voucher_table}_pkey", new_voucher_table, ["id"])

    # # Create referenced FK from voucher_update table
    # op.create_foreign_key("voucher_update_voucher_id_fkey", "voucher_update", "voucher", ["voucher_id"], ["id"], ondelete="CASCADE") # reward_update table

    # Re-create indices
    op.create_unique_constraint("voucher_code_retailer_slug_voucher_config_unq", new_voucher_table, ["voucher_code", "retailer_slug", "voucher_config_id"])
    
    op.create_index("ix_voucher_retailer_slug", new_voucher_table, ["retailer_slug"])
    op.create_index("ix_voucher_voucher_code", new_voucher_table, ["voucher_code"])


    
    # VoucherConfig

    old_reward_config_table = "reward_config"
    new_voucher_config_table = "voucher_config"

    # Drop indices
    for index in ("ix_reward_config_retailer_slug", "ix_reward_config_reward_slug"):
        op.drop_index(op.f(index), table_name=old_reward_config_table)

    op.drop_constraint("reward_slug_retailer_slug_unq", table_name=new_voucher_config_table)
    
    # rename voucher_config table -> reward_config
    # op.rename_table(old_reward_config_table, new_voucher_config_table)

    # Drop PK from existing table
    existing_table = sa.Table(new_voucher_config_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_voucher_config_table)

    # Re-create PK
    op.create_primary_key(f"{new_voucher_config_table}_pkey", new_voucher_config_table, ["id"])
    op.create_foreign_key(f"{new_voucher_table}_voucher_config_id_fkey", new_voucher_table, referent_table, ["voucher_config_id"], ["id"])

    # Alter sequence
    op.execute(f"ALTER SEQUENCE {old_reward_config_table}_id_seq RENAME TO {new_voucher_config_table}_id_seq;")

    # Alter enums
    op.execute("ALTER TYPE rewardfetchtype RENAME TO voucherfetchtype")
    
    # Alter columns
    op.alter_column(table_name=new_voucher_config_table, column_name="reward_slug", new_column_name="voucher_type_slug")    

    # Re-create indices
    op.create_unique_constraint("voucher_type_slug_retailer_slug_unq", new_voucher_config_table, ["voucher_type_slug", "retailer_slug"])
    
    op.create_index("ix_voucher_config_retailer_slug", new_voucher_config_table, ["retailer_slug"])
    op.create_index("ix_voucher_config_voucher_type_slug", new_voucher_config_table, ["voucher_type_slug"])


    # VoucherUpdate

    old_reward_update_table = "reward_update"
    new_voucher_update_table = "voucher_update"
    
    # rename voucher_config table -> reward_config
    op.rename_table(old_reward_update_table, new_voucher_update_table)

    # Drop PK from existing table
    existing_table = sa.Table(new_voucher_update_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_voucher_update_table)
    # op.drop_constraint(existing_table.foreign_key_constraints, new_voucher_update_table)

     # Alter columns
    op.alter_column(table_name=new_voucher_update_table, column_name="reward_uuid", new_column_name="voucher_id")    

    # Re-create FK and PK
    referent_reward_table = "voucher" ##TODO: This depends on voucher table rename
    op.create_foreign_key(f"{new_voucher_update_table}_voucher_id_fkey", new_voucher_update_table, referent_reward_table, ["voucher_id"], ["id"], ondelete="CASCADE",)
    op.create_primary_key(f"{new_voucher_update_table}_pkey", new_voucher_update_table, ["id"])

    # Alter sequence
    op.execute(f"ALTER SEQUENCE {old_reward_update_table}_id_seq RENAME TO {new_voucher_update_table}_id_seq;")
    


    # VoucherFileLog

    old_reward_file_log_table = "reward_file_log"
    new_voucher_file_log_table = "voucher_file_log"

    # Drop indices
    for index in (f"ix_{old_reward_file_log_table}_file_name", f"ix_{old_reward_file_log_table}_file_agent_type"):
        op.drop_index(op.f(index), table_name=old_reward_file_log_table)
    # op.drop_constraint("file_name_file_agent_type_unq", table_name=old_voucher_file_log_table)

    # rename voucher_file_log table -> reward_file_log
    op.rename_table(old_reward_file_log_table, new_voucher_file_log_table)

    # Drop PK from existing table
    existing_table = sa.Table(new_voucher_file_log_table, existing_metadata, autoload_with=conn)
    op.drop_constraint(existing_table.primary_key.name, new_voucher_file_log_table)

    # Re-create PK
    op.create_primary_key(f"{new_voucher_file_log_table}_pkey", new_voucher_file_log_table, ["id"])

    # Re-create indices
    # op.create_unique_constraint("file_name_file_agent_type_unq", new_reward_file_log_table, ["file_name", "file_agent_type"])
    op.create_index(f"ix_{new_voucher_file_log_table}_file_name", new_voucher_file_log_table, ["file_name"])
    op.create_index(f"ix_{new_voucher_file_log_table}_file_agent_type", new_voucher_file_log_table, ["file_agent_type"])

    # Alter sequence
    op.execute(f"ALTER SEQUENCE {old_reward_file_log_table}_id_seq RENAME TO {new_voucher_file_log_table}_id_seq;")
