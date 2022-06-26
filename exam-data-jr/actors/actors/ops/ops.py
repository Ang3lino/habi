
from collections import defaultdict
from dagster import op

import numpy as np
import actors.utils as utils


@op 
def dense_matrix_as_defaultdict(mat: np.ndarray) -> defaultdict:
    return utils.dense_matrix_as_defaultdict(mat)

@op(required_resource_keys={"excel"})
def get_actor_names(context) -> list:
    xls = context.resources.excel.get()
    return utils.get_actor_names(xls)

@op(required_resource_keys={"excel"})
def get_matrix_from_excel(context) -> np.ndarray:
    xls = context.resources.excel.get()
    res = utils.get_matrix_from_excel(xls)
    context.log.info(res)
    return res

@op(required_resource_keys={"mysql"})
def get_old_table(context) -> defaultdict:
    db = context.resources.mysql
    return db.get_old_table()

@op(required_resource_keys={"mysql"})
def load_actors(context, actors: list) -> None:
    db = context.resources.mysql
    db.load_actors(actors)

@op(required_resource_keys={"mysql"})
def update_relations(context, old_t: defaultdict, new_t: defaultdict) -> None:
    db = context.resources.mysql
    db.update_relations(old_t, new_t)

@op
def hi_op(context):
    context.log.info("WASA")
