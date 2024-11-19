from sports.resources.duckdb import MotherDuck
from sports.utils import defined_resource
from dagster import EnvVar

@defined_resource
def motherduck():
    return MotherDuck(
        database=EnvVar("MOTHERDUCK_DATABASE"),
    )
