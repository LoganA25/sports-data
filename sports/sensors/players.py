from dagster import schedule, ScheduleEvaluationContext, RunRequest
from sports.assets.players import players_job


@schedule(
    job=players_job, cron_schedule="0 1 * * *", execution_timezone="US/Central"
)
def office_users(context: ScheduleEvaluationContext):
    """
    This schedule function triggers the 'players_job' to run daily at 1:00AM CST.
    """
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=None,
        tags={"date": scheduled_date},
    )
