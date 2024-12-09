from dagster import asset, define_asset_job

import requests
import pandas as pd


@asset(group_name="nfl", compute_kind="Python", io_manager_key="motherduck", key_prefix="nfl",)
def nfl_schedules() -> pd.DataFrame:

    teams = [
        "ARI",
        "ATL",
        "BAL",
        "BUF",
        "CAR",
        "CHI",
        "CIN",
        "CLE",
        "DAL",
        "DEN",
        "DET",
        "GB",
        "HOU",
        "IND",
        "JAX",
        "KC",
        "LAC",
        "LAR",
        "LV",
        "MIA",
        "MIN",
        "NE",
        "NO",
        "NYG",
        "NYJ",
        "PHI",
        "PIT",
        "SEA",
        "SF",
        "TB",
        "TEN",
        "WAS",
    ]

    all_schedules = []

    for team in teams:
        try:
            url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team}/schedule?year=2024"

            response = requests.get(url)

            schedule = response.json()

            # team_abbreviation = schedule["team"]["abbreviation"]

            weeks = [event["week"]["number"] for event in schedule["events"]]

            opponent = []

            home_away = []

            for event in schedule["events"]:
                competitors = event["competitions"][0]["competitors"]
                for comp in competitors:
                    if comp["team"]["abbreviation"] == team:
                        home_away.append(comp["homeAway"])
                    else:
                        opponent.append(comp["team"]["abbreviation"])

            weeks_opponent = list(zip(weeks, opponent, home_away))

            df = pd.DataFrame(weeks_opponent, columns=["Week", "PlayerOpponent", "Location"])

            df["Team"] = team

            all_schedules.append(df)
        
        except Exception as e:
            print(f"Error for team {team}: {e}")

    df = pd.concat(all_schedules)

    return df


nfl_schedules_job = define_asset_job(name="nfl_schedules", selection=[nfl_schedules])
