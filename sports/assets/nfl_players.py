from dagster import asset, define_asset_job, Config, StaticPartitionsDefinition
from typing import List
from io import BytesIO

import requests
import pandas as pd

class Dates(Config):
    week: str

players_partitions = StaticPartitionsDefinition(
    partition_keys=[
        "week_1",
        "week_2",
        "week_3",
        "week_4",
        "week_5",
        "week_6",
        "week_7",
        "week_8",
        "week_9",
        "week_10",
        "week_11",
        "week_12",
        "week_13",
        "week_14",
        "week_15",
        "week_16",
        "week_17",
        "week_18",
    ]
)

@asset(metadata={"partition_expr": "week"}, partitions_def=players_partitions, group_name="nfl_players", compute_kind="Python", io_manager_key="motherduck", key_prefix="teams")
def nfl_players(config: Dates) -> pd.DataFrame:

    postitions = ["QB", "WR", "RB", "TE", "K", "DB", "LB"]

    all_players = []

    for postition in postitions:
        url = f"https://raw.githubusercontent.com/hvpkod/NFL-Data/main/NFL-data-Players/2024/{config.week}/{postition}.json"

        file = BytesIO(requests.get(url).content)

        df = pd.read_json(file)

        all_players.append(df)

    df = pd.concat(all_players)

    df["week"] = config.week

    return df


nfl_players_job = define_asset_job(name="teams", selection=[nfl_players])