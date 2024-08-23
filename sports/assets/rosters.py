from dagster import asset, define_asset_job
import requests, pandas as pd, os


@asset(group_name='rosters', compute_kind='Python', io_manager_key='motherduck', key_prefix=['rosters'])
def rosters(context) -> pd.DataFrame:
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeamRoster"

    all_teams = []

    teams = [
        {"teamID": "1", "teamAbv": "ARI"},
        {"teamID": "2", "teamAbv": "ATL"},
        {"teamID": "3", "teamAbv": "BAL"},
        {"teamID": "4", "teamAbv": "BUF"},
        {"teamID": "5", "teamAbv": "CAR"},
        {"teamID": "6", "teamAbv": "CHI"},
        {"teamID": "7", "teamAbv": "CIN"},
        {"teamID": "8", "teamAbv": "CLE"},
        {"teamID": "9", "teamAbv": "DAL"},
        {"teamID": "10", "teamAbv": "DEN"},
        {"teamID": "11", "teamAbv": "DET"},
        {"teamID": "12", "teamAbv": "GB"},
        {"teamID": "13", "teamAbv": "HOU"},
        {"teamID": "14", "teamAbv": "IND"},
        {"teamID": "15", "teamAbv": "JAX"},
        {"teamID": "16", "teamAbv": "KC"},
        {"teamID": "17", "teamAbv": "LV"},
        {"teamID": "18", "teamAbv": "LAC"},
        {"teamID": "19", "teamAbv": "LAR"},
        {"teamID": "20", "teamAbv": "MIA"},
        {"teamID": "21", "teamAbv": "MIN"},
        {"teamID": "22", "teamAbv": "NE"},
        {"teamID": "23", "teamAbv": "NO"},
        {"teamID": "24", "teamAbv": "NYG"},
        {"teamID": "25", "teamAbv": "NYJ"},
        {"teamID": "26", "teamAbv": "PIT"},
        {"teamID": "27", "teamAbv": "PHI"},
        {"teamID": "28", "teamAbv": "SF"},
        {"teamID": "29", "teamAbv": "SEA"},
        {"teamID": "30", "teamAbv": "TB"},
        {"teamID": "31", "teamAbv": "TEN"},
        {"teamID": "32", "teamAbv": "WSH"},
    ]

    for team in teams:
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

rosters_job = define_asset_job(name="rosters", selection=[rosters])