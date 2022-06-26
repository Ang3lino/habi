from actors.ops.ops import *
from actors import utils

import os


fname = 'Matriz_de_adyacencia_data_team.xlsx'
xls = utils.LocalExcel(fname).get()


def test_get_matrix_from_excel():
    res = utils.get_matrix_from_excel(xls)
    m, n = res.shape
    assert m == n

def test_local_mysql():
    try:
        db = utils.LocalMySql()
        assert True
    except Exception as e:
        assert False  # cannot connect to mysql

def test_update_relations():
    db = utils.LocalMySql()
    old_t = db.get_old_table()
    new_t = utils.get_random_defaultdict(1, 50)
    db.update_relations(old_t, new_t, inplace=False)
    cur_t = db.get_old_table()
    db.conn.rollback()
    for i in new_t.keys():
        assert new_t[i] == cur_t[i]
