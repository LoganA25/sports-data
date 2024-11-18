from dagster import asset, define_asset_job
from io import BytesIO

import requests
import pandas as pd

@asset(group_name='teams', compute_kind='Python', io_manager_key='motherduck', key_prefix='teams')
def players(context) -> pd.DataFrame:

    postitions = ["QB", "WR", "RB", "TE", "K", "DB", "LB"]

    all_players = []

    for postition in postitions:
        url = f"https://raw.githubusercontent.com/hvpkod/NFL-Data/main/NFL-data-Players/2024/1/{postition}.json"

        file = BytesIO(requests.get(url).content)

        df = pd.read_json(file)

        all_players.append(df)

    df = pd.concat(all_players)

    return df

players_job = define_asset_job(name="teams", selection=[players])
