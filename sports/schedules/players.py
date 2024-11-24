# from dagster import sensor, SensorEvaluationContext, SensorResult, RunRequest
from dagster import schedule, ScheduleEvaluationContext, RunRequest
from sports.assets.players import players_job
from sports.assets.players import players_partitions

import requests


@schedule(job=players_job, name="players", cron_schedule="0 1 * * 3", execution_timezone="US/Central")
def players(context: ScheduleEvaluationContext):

    positions = ["QB", "WR", "RB", "TE", "K", "DB", "LB"]

    nfl_weeks = ["1", "2"]

    partition_keys = players_partitions.get_partition_keys(dynamic_partitions_store=context.instance)

    print(partition_keys)

    for week in nfl_weeks:
        all_positions_found = True
        for position in positions:
            url = (f"https://raw.githubusercontent.com/hvpkod/NFL-Data/main/NFL-data-Players/2024/{week}/{position}.json")
            try:
                response = requests.get(url)
                if response.status_code != 200:
                    context.log.info(f"Missing data for: {week} for position: {position}")
                    all_positions_found = False    
                    break # Skip to the next week if any position is missing
                else:
                    context.log.info(f"Found data for week:{week} position: {position}")

            except Exception as e:
                context.log.error(f"Error fetching week: {week}, position: {position}, error: {e}")
                all_positions_found = False
                break

        if all_positions_found:
            context.log.info(f"Found data for all positions for week: {week}")
            return [
            RunRequest(
                run_key=partition_key,
                run_config={"ops": {"teams__players": {"config": {"week": partition_key}}}},
                tags={"dagster/partition": partition_key},
            )
            for partition_key in partition_keys
        ]

    else:
        context.log.info(f"No data found for weeks: {', '.join(nfl_weeks)}")
      
