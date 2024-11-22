from dagster import sensor, SensorEvaluationContext, SensorResult, RunRequest
from sports.assets.players import players_job
from sports.assets.players import players_partitions

import requests

@sensor(job=players_job, name="players", minimum_interval_seconds=60)
def players(context: SensorEvaluationContext):

    positions = ["QB", "WR", "RB", "TE", "K", "DB", "LB"]

    weeks = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18"]

    for week in weeks:
        week_found = False

        for position in positions:
            url = (f"https://raw.githubusercontent.com/hvpkod/NFL-Data/main/NFL-data-Players/2024/{week}/{position}.json")

            try:
                if requests.get(url).status_code == 200:
                    context.log.info(f"Found week: {week} for position: {position}")
                    week_found = True
            
                    return SensorResult(
                        run_requests = [RunRequest(
                        run_key=week,
                        run_config={"ops": {"teams__players": {"config": {"week": week}}}},
                        tags={"dagster/partition": week})
                        ],
                        dynamic_partitions_requests=[players_partitions.build_add_request([week])]
                    )
                else:
                    context.log.info(f"Did not find week: {week} for position: {position}")

            except Exception as e:
                context.log.error(f"Error fetching week: {e}")

        if not week_found:
            context.log.info(f"Did not find week: {week} for any position")
            break
