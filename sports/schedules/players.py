# from dagster import sensor, SensorEvaluationContext, SensorResult, RunRequest
from dagster import schedule, ScheduleEvaluationContext, RunRequest
from sports.assets.players import players_job
from sports.assets.players import players_partitions

import requests


@schedule(job=players_job, name="players", cron_schedule="0 1 * * 2", execution_timezone="US/Central")
def players(context: ScheduleEvaluationContext):
    positions = ["QB", "WR", "RB", "TE", "K", "DB", "LB"]
    nfl_weeks = ["1", "2"]

    run_requests = []

    for week in nfl_weeks:
        try:
            missing_positions = []
            for position in positions:
                url = f"https://raw.githubusercontent.com/hvpkod/NFL-Data/main/NFL-data-Players/2024/{week}/{position}.json"
                response = requests.get(url)
                if response.status_code != 200:
                    missing_positions.append(position)

            if missing_positions:
                context.log.warning(
                    f"Missing data for week {week}: positions {', '.join(missing_positions)}"
                )
            else:
                context.log.info(f"All data available for week {week}.")
                run_requests.append(
                    RunRequest(
                        run_key=week,
                        run_config={
                            "ops": {"teams__players": {"config": {"week": week}}}
                        },
                        tags={"dagster/partition": week},
                    )
                )

        except Exception as e:
            context.log.error(f"Error processing week {week}: {e}")

    if run_requests:
        return run_requests
    else:
        context.log.info(f"No data found for weeks {', '.join(nfl_weeks)}")
        return []
