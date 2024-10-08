from dagster import EnvVar
from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager
from sports.utils import defined_resource
import os

@defined_resource
def motherduck() -> DuckDBPandasIOManager:
    return DuckDBPandasIOManager(
            database=f"md:raw?motherduck_token={os.getenv('MOTHERDUCK_TOKEN')}"
        )
