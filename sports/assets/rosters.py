from dagster import asset, define_asset_job

import requests
import pandas as pd
import os
import yaml
import duckdb

with open("./sports/configs/nfl_teams.yml") as file:
    nfl_teams: dict = yaml.load(file, Loader=yaml.FullLoader).get("teams")

@asset(group_name='teams', compute_kind='Python', io_manager_key='motherduck', key_prefix='teams')
def rosters(context) -> pd.DataFrame:
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeamRoster"

    all_teams = []

    for team in nfl_teams:
        querystring = {
                "teamID": team['teamID'],
                "teamAbv": team['teamAbv'],
                "getStats": "true",
                "fantasyPoints": "true",
            }

        headers = {
                "x-rapidapi-key": os.getenv("RAPID_API_KEY"),
                "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com",
            }

        response = requests.get(url, headers=headers, params=querystring)

        response = response.json()

        df = pd.DataFrame(response["body"], index=None)

        all_teams.append(df)

    df = pd.concat(all_teams)

    roster_json = df['roster'].to_json(orient="records")
    
    with open("./sports/assets/roster_data.json", "w") as json_file:
        json_file.write(roster_json)

    df_roster = duckdb.read_json("./sports/assets/roster_data.json")

    df = df.join(df_roster)

    return df

rosters_job = define_asset_job(name="teams", selection=[rosters])
