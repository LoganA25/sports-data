# import os

# from dagster_dbt import load_assets_from_dbt_cloud_job
# from dagster_duckdb_pandas import duckdb_pandas_io_manager

# from dagster import (
#     Definitions,
#     ScheduleDefinition,
#     define_asset_job,
#     fs_io_manager,
#     load_assets_from_package_module,
# )
# from dagster._utils import file_relative_path

# DBT_PROJECT_DIR = file_relative_path(__file__, "../dbt_project")
# DBT_PROFILES_DIR = file_relative_path(__file__, "../dbt_project/config")

# # all assets live in the default dbt_schema
# dbt_assets = load_assets_from_dbt_project(
#     DBT_PROJECT_DIR,
#     DBT_PROFILES_DIR,
#     # prefix the output assets based on the database they live in plus the name of the schema
#     key_prefix=["duckdb", "dbt_schema"],
#     # prefix the source assets based on just the database
#     # (dagster populates the source schema information automatically)
#     source_key_prefix=["duckdb"],
# )

# resources = {
#     # this io_manager allows us to load dbt models as pandas dataframes
#     "io_manager": duckdb_pandas_io_manager.configured(
#         {"database": os.path.join(DBT_PROJECT_DIR, "example.duckdb")}
#     ),
#     # this io_manager is responsible for storing/loading our pickled machine learning model
#     "model_io_manager": fs_io_manager,
#     # this resource is used to execute dbt cli commands
#     "dbt": dbt_cli_resource.configured(
#         {"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROFILES_DIR}
#     ),
# }

