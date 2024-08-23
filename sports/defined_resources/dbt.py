import os
from dagster_dbt import DbtCliResource
from sports.constants import dbt_project_dir
from sports.utils import defined_resource


@defined_resource
def dbt():
    return DbtCliResource(
        project_dir=os.fspath(dbt_project_dir),
        target="prod",
    )
