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
    {
        mod.name: mod.resource
        for mod in load_from_package_module(defined_resources, DefinedResource)
    },
)
