from dagster import schedule, ScheduleEvaluationContext, RunRequest

from actors.jobs.jobs import hi_job, launcher


@schedule(job=hi_job, cron_schedule="* * * * *")
def each_min(context: ScheduleEvaluationContext):
    return RunRequest(
        run_key=None,
    )

@schedule(
        job=launcher, 
        cron_schedule="0 0 * * *", 
        execution_timezone="America/Bogota")
def each_day():
    return RunRequest(
        run_key=None,
    )