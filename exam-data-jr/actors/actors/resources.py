
from dagster import resource

from actors.utils import LocalExcel, LocalMySql


@resource(config_schema={"excel_name": str})
def local_excel(context):
    return LocalExcel(context.resource_config["excel_name"])

@resource(config_schema={
        "username": str,
        "password": str,
        "hostname": str,
        "db_name": str
        })
def local_mysql(context):
    return LocalMySql(context.resource_config)
    