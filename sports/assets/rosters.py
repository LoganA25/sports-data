from dagster import asset, define_asset_job

import requests
import pandas as pd
import os
import yaml

with open("./sports/configs/nfl_teams.yml") as file:
    nfl_teams: dict = yaml.load(file, Loader=yaml.FullLoader).get("teams")

@asset(group_name='teams', compute_kind='Python', io_manager_key='motherduck', key_prefix='teams')
def rosters(context) -> pd.DataFrame:
    """
    Retrieves NFL team roster data from an API and processes it into a dataframe.

    This function fetches roster information for each NFL team based on details from a yaml file.
    For each team, it gathers player statistics, injury reports, and general team data. The data is parsed and
    organized into a single, comprehensive table, with each team's information combined into one dataset.
    """
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeamRoster"

    final_dfs = []

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
        try:
            response = requests.get(url, headers=headers, params=querystring)

            response = response.json()

            df1 = pd.DataFrame(response["body"], index=None)
            df1 = df1.drop(columns=["roster"])

            df2 = pd.DataFrame(response["body"]["roster"], index=None)

            stats_normalized = pd.json_normalize(df2["stats"])
            injurys_normalized = pd.json_normalize(df2["injury"])

            df2= pd.concat([df2, stats_normalized, injurys_normalized], axis=1)
            df2 = df2.drop(columns=["teamAbv", "stats", "injury"])

            final_team_df = pd.concat([df1, df2], axis=1)

            final_team_df = final_team_df.loc[:, ~final_team_df.columns.duplicated()]

            final_dfs.append(final_team_df)
        
        except Exception as e:
            context.log.error(f"An error occurred for {team['teamAbv']}: {e}")

    final_df_all_teams = pd.concat(final_dfs, axis=0, ignore_index=True)

    return final_df_all_teams


rosters_job = define_asset_job(name="teams", selection=[rosters])
