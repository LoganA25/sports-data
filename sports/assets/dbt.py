from pathlib import Path
from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets
from typing import Any, Mapping, Optional
import os
from ..constants import dbt_manifest_path

manifest_path = Path(dbt_manifest_path)


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:

        schema = dbt_resource_props.get("schema")

        return schema

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:

        asset_key = super().get_asset_key(dbt_resource_props)

        if dbt_resource_props.get("resource_type") == "source":
            schema = dbt_resource_props.get("schema")

            # if os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME") != "prod":
            #     schema = f"dev_{schema}"

            asset_key = AssetKey([schema, dbt_resource_props.get("name")])

        return asset_key


@dbt_assets(
    manifest=manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
