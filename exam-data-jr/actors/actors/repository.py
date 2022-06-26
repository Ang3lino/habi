# from dagster import repository

# from actors.jobs.say_hello import say_hello_job
# from actors.schedules.my_hourly_schedule import my_hourly_schedule
# from actors.sensors.my_sensor import my_sensor


# @repository
# def actors():
#     """
#     The repository definition for this actors Dagster repository.

#     For hints on building your Dagster repository, see our documentation overview on Repositories:
#     https://docs.dagster.io/overview/repositories-workspaces/repositories
#     """
#     jobs = [say_hello_job]
#     schedules = [my_hourly_schedule]
#     sensors = [my_sensor]

#     return jobs + schedules + sensors

from dagster import repository
from actors.jobs.jobs import launcher
from actors.schedules.schedules import each_day


@repository
def actors():
    return [launcher, each_day]