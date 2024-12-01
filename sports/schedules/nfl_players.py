# from dagster import sensor, SensorEvaluationContext, SensorResult, RunRequest
from dagster import schedule, ScheduleEvaluationContext, RunRequest
from sports.assets.nfl_players import nfl_players_job

import requests

@schedule(job=nfl_players_job, name="nfl_players", cron_schedule="0 1 * * 2", execution_timezone="US/Central")
def players(context: ScheduleEvaluationContext):
    positions = ["QB", "WR", "RB", "TE", "K", "DB", "LB"]
    nfl_weeks = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18"]

    run_requests = []

    for week in nfl_weeks:
        try:
            missing_positions = []
            for position in positions:
                url = f"https://raw.githubusercontent.com/hvpkod/NFL-Data/main/NFL-data-Players/2024/{week}/{position}.csv"
                response = requests.get(url)
                if response.status_code != 200:
                    missing_positions.append(position)

            if missing_positions:
                context.log.warning(
                    f"Missing data for week {week} for these position(s): {', '.join(missing_positions)}"
                )
                break
            else:
                context.log.info(f"All data available for week {week}.")
                run_requests.append(
                    RunRequest(
                        run_key=f"week_{week}",
                        run_config={
                            "ops": {"nfl__players": {"config": {"week": week}}}
                        },
                        tags={"dagster/partition": f"week_{week}"},
                        partition_key=f"week_{week}",
                    )
                    
                )

        except Exception as e:
            context.log.error(f"Error processing week {week}: {e}")

    if run_requests:
        return run_requests
    else:
        context.log.info(f"No data found for weeks {', '.join(nfl_weeks)}")
        return []
