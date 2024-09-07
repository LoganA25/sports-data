from dagster import asset, define_asset_job
import requests, pandas as pd, os, yaml

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

    return df

rosters_job = define_asset_job(name="teams", selection=[rosters])
