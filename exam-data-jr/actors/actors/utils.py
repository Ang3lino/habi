import pandas as pd
import numpy as np

import os
import typing
import pymysql
import random

from collections import defaultdict


class LocalExcel:
    def __init__(self, fname):
        self.excel = pd.ExcelFile(os.path.join('res', fname))

    def get(self) -> pd.ExcelFile:
        return self.excel


class LocalMySql:
    def __init__(self, creds: dict=None):
        if creds:
            self.conn = pymysql.connect(
                    host=creds['hostname'], 
                    user=creds['username'], 
                    password=creds['password'], 
                    database=creds['db_name'])
        else:
            self.conn = pymysql.connect(
                    host="localhost", user="root", password="root", database="actors")

    def get_connection(self):
        return self.conn

    def get_cursor(self):
        return self.conn.cursor()

    def load_actors(self, actors: typing.List):
        conn, cursor = self.conn, self.get_cursor()
        cursor = conn.cursor()
        conn.begin()
        try:
            for i, name in enumerate(actors):
                cursor.callproc('usp_actor_add', (i + 1, name))
            conn.commit()
        except Exception as e:
            print("Ya se cargaron previamente los actores (%s,%s)" % (i + 1, name))
            print(e)

    def read(self, query):
        cursor = self.get_cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def get_old_table(self) -> defaultdict:
        read =  self.read
        result = read("SELECT id, actor_id FROM actor_relation")
        relations = defaultdict(set)
        for i, j in result:
            relations[i].add(j)
        return relations

    def update_relations(self, old_table: defaultdict, new_table: defaultdict,
            inplace=True) -> None:
        conn = self.conn
        conn.begin()
        cursor = conn.cursor()
        idx = set(old_table.keys()) | set(new_table.keys())  # barre sobre cualquier cambio significativo en fila
        for i in idx:
            a, b = old_table[i], new_table[i]
            old_row = a - b  # se conservan los elementos sin cambio
            new_row = b - a
            for j in old_row:
                write(cursor, "DELETE FROM actor_relation WHERE id = %s AND actor_id = %s", (i, j))
            for j in new_row:
                cursor.callproc('usp_actor_relation_add', (i, j))
        if inplace:
            conn.commit()

    def close(self):
        if self.conn.open:
            self.conn.close()

def write(cursor, query, args=None):
    if args:
        cursor.execute(query, args)
    else:
        cursor.execute(query)

def get_random_table(m: int) -> np.ndarray:
    result = np.zeros((m, m))
    for i in range(m):
        for j in range(m):
            result[i][j] = random.randint(0, 1)
    return result

# for testing
def get_random_defaultdict(a: int, b: int) -> defaultdict:
    new_table = defaultdict(set)
    for i in range(a, b + 1):
        for j in range(6):
            new_table[i].add(random.randint(a, b))
    return new_table

def get_actor_names(xls: pd.ExcelFile) -> list:
    df = pd.read_excel(xls, 'Lista de actores', header=3, usecols="C")
    return df.values.squeeze().tolist()

def get_matrix_from_excel(xls: pd.ExcelFile) -> np.ndarray:
    df = pd.read_excel(xls, sheet_name='Matriz de adyacencia')
    return df.iloc[1:, 2:].values

def dense_matrix_as_defaultdict(mat: np.ndarray) -> defaultdict:
    result = defaultdict(set)
    m = mat.shape[0]
    for i in range(m):
        for j in range(i, m):  # empieza en i para no rebuscar
            if mat[i][j] == 1:  # ignora 0, np.nan
                result[i + 1].add(j + 1)
    return result
