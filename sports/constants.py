import os
from pathlib import Path

from dagster_dbt import DbtCliResource

# We expect the dbt project to be installed as package data.
# For details, see https://docs.python.org/3/distutils/setupscript.html#installing-package-data.
dbt_project_dir = Path(__file__).joinpath("..", "..", "dbt_project").resolve()

dbt = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at runtime.
# Otherwise, we expect a manifest to be present in the project's target directory.

target = 'prod' 

dbt_parse_invocation = dbt.cli(["parse", "--target", target], manifest={}).wait()

dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")