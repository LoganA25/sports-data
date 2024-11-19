from dagster_duckdb_pandas import DuckDBPandasIOManager

class MotherDuck(DuckDBPandasIOManager):
    def motherduck() -> DuckDBPandasIOManager:
        return MotherDuck()
