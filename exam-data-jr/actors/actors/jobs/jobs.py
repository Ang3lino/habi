from dagster import job

from actors.ops.ops import *
from actors.resources import local_excel, local_mysql


@job(resource_defs={
    "excel": local_excel, 
    "mysql": local_mysql
})
def launcher():
    names = get_actor_names()
    new_t = get_matrix_from_excel()
    old_t = get_old_table()
    load_actors(names)
    new_t = dense_matrix_as_defaultdict(new_t)
    update_relations(old_t, new_t)
    print("ejecutado con codigo 0")

@job
def hi_job():
    hi_op()
    print('hola')