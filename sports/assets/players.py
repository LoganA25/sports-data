from dagster import asset, define_asset_job
from io import BytesIO

import requests
import pandas as pd
import yaml


with open("./sports/configs/nfl_teams.yml") as file:
    nfl_teams: dict = yaml.load(file, Loader=yaml.FullLoader).get("teams")

@asset(group_name='teams', compute_kind='Python', io_manager_key='motherduck', key_prefix='teams')
def players(context) -> pd.DataFrame:

    postitions = ["QB", "WR", "RB", "TE", "K", "DB", "LB"]

    all_players = []

    for player in postitions:
        url = f"https://raw.githubusercontent.com/hvpkod/NFL-Data/main/NFL-data-Players/2024/1/{player}.json"

        file = BytesIO(requests.get(url).content)

        df = pd.read_json(file)

        all_players.append(df)

    df = pd.concat(all_players)

    return df
# """
# Retrieves and processes NFL team roster data from an API into a pandas DataFrame.

# This function fetches roster information for each NFL team using details from a YAML configuration file.
# It gathers player statistics, injury reports, and general team data for each team.

# Args:
#     context (OpExecutionContext): Dagster context object for logging and job metadata.

# Returns:
#     pd.DataFrame: A DataFrame containing roster data for all NFL teams, with normalized player statistics
#     and injury reports.

# Notes:
#     - The 'nfl_teams' dictionary should contain 'teamID' and 'teamAbv' for each team.
# """
# url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeamRoster"

# final_dfs = []

# for team in nfl_teams:
#     querystring = {
#             "teamID": team['teamID'],
#             "teamAbv": team['teamAbv'],
#             "getStats": "true",
#             "fantasyPoints": "true",
#         }

#     headers = {
#             "x-rapidapi-key": os.getenv("RAPID_API_KEY"),
#             "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com",
#         }
#     try:
#         response = requests.get(url, headers=headers, params=querystring)

#         response = response.json()

#         df1 = pd.DataFrame(response["body"], index=None)
#         df1 = df1.drop(columns=["roster"])

#         df2 = pd.DataFrame(response["body"]["roster"], index=None)

#         stats_normalized = pd.json_normalize(df2["stats"])
#         injurys_normalized = pd.json_normalize(df2["injury"])

#         df2= pd.concat([df2, stats_normalized, injurys_normalized], axis=1)
#         df2 = df2.drop(columns=["teamAbv", "stats", "injury"])

#         final_team_df = pd.concat([df1, df2], axis=1)

#         final_team_df = final_team_df.loc[:, ~final_team_df.columns.duplicated()]

#         final_dfs.append(final_team_df)

#     except Exception as e:
#         context.log.error(f"An error occurred for {team['teamAbv']}: {e}")

# final_df_all_teams = pd.concat(final_dfs, axis=0, ignore_index=True)

# return final_df_all_teams


players_job = define_asset_job(name="teams", selection=[players])
