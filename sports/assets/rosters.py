from dagster import asset
import requests, pandas as pd, os


@asset(group_name='rosters', compute_kind='Python', io_manager_key='duckdb', key_prefix=['rosters'])
def rosters(context):
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeamRoster"

    querystring = {
            "teamID": "23",
            "teamAbv": "NO",
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

    return df


