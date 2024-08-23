from dagster import Definitions
from .utils import load_from_package_module, DefinedResource
from dagster_duckdb_pandas import DuckDBPandasIOManager
# from . resources.motherduck import MotherduckIOManager, DuckDB
import os

from . import assets, jobs, sensors, schedules, defined_resources
from dagster._core.definitions import (
    AssetsDefinition,
    JobDefinition,
    SensorDefinition,
    ScheduleDefinition,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

defs = Definitions(
    assets=load_from_package_module(assets, AssetsDefinition),
    jobs=[
        *load_from_package_module(jobs, JobDefinition),
        *load_from_package_module(assets, UnresolvedAssetJobDefinition),
    ],
    schedules=load_from_package_module(schedules, ScheduleDefinition),
    sensors=load_from_package_module(sensors, SensorDefinition),
    resources=
    # resources={
    #     "duckdb": DuckDBPandasIOManager(
    #         database="md:?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImxvZ2FuZGFsbGVuMjVAZ21haWwuY29tIiwic2Vzc2lvbiI6ImxvZ2FuZGFsbGVuMjUuZ21haWwuY29tIiwicGF0IjoidHhQYTh2ckwyeWVLVDVxSzE5RGVYVm1hUVVjNHY1aFJvZjdubnJaRmhhTSIsInVzZXJJZCI6IjFhNjY5ODhjLTM5ZjctNGVhOS1hN2QyLTQ1ZmVlODMwNmVhMiIsImlzcyI6Im1kX3BhdCIsImlhdCI6MTcyNDA0MzEwOX0.pUR0uRS2cmYPIj_xScc1P81rbDccmhf1gMd5H82R2Uc",
    #         schema="raw.teams",
    #     ),
    # },
    {
        mod.name: mod.resource
        for mod in load_from_package_module(defined_resources, DefinedResource)
    },
)
