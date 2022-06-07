
import pandas as pd
import os
import mysql.connector

from decimal import getcontext
from enum import unique
from string import printable
from mysql.connector import Error
from tqdm import tqdm
from pprint import pprint


def get_conn():
    return mysql.connector.connect(
            host='192.168.0.217',
            database='propiedades',
            user='root',
            password='propiedades')

def test_connection():
    try:
        connection = get_conn()
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


connection = get_conn()
cursor = connection.cursor()


def read_all(query, cursor=cursor):
    cursor.execute(query)
    return cursor.fetchall()

def print_all(query, cursor=cursor):
    records = read_all(query, cursor)
    pprint(records)


print_all("SHOW TABLES;")
print_all("DESC properties")
print_all("SELECT colony FROM properties LIMIT 10")

varchar_fields = [x for x in read_all("DESC properties") if 'varchar' in str(x[1])]
unique_states = [('Tamaulipas',), ('México',), ('Sinaloa',), ('Campeche',), ('DF / CDMX',), ('Coahuila de Zaragoza',), ('Chihuahua',), ('Durango',), ('Jalisco',), ('Colima',), ('Veracruz de Ignacio de la Llave',), ('Guanajuato',), ('Puebla',), ('Sonora',), ('Oaxaca',), (None,), ('Chiapas',), ('Quintana Roo',), ('Michoacán de Ocampo',), ('Baja California Sur',), ('Aguascalientes',), ('Baja California',), ('Yucatán',), ('Nayarit',), ('Guerrero',), ('Morelos',), ('Nuevo León',), ('Querétaro',), ('Hidalgo',), ('Tlaxcala',), ('Tabasco',), ('San Luis Potosí',), ('Zacatecas',), ('Distrito Federal',)]
result= read_all('DESC properties', cursor)
fields = [t[0] for t in result]


def download_csv_from_mysql(cursor, unique_states, fields):
    n = len(unique_states)
    pbar = tqdm(unique_states, total=n)
    for state in pbar:
        state = state[0]
        query = f"SELECT {', '.join(fields)} FROM properties WHERE state = '{state}'"
        res = read_all(query, cursor)
        df = pd.DataFrame(data=res)
        df.columns = fields
        df.to_csv(os.path.join('csv', f'{state}.csv'))


df = pd.read_csv("./sepomex/CPdescarga.csv", sep="|", encoding='utf8')
subset = df[df["d_estado"] == "Aguascalientes"]

cols = ['d_codigo', 'd_asenta']
subset[cols]


import operator
from difflib import SequenceMatcher


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


df = pd.read_csv("csv/Tamaulipas.csv", )
cols = ['id', 'colony']
values = df[cols].values
idx = list(map(operator.itemgetter(0), values))
colonies = list(map(operator.itemgetter(1), values))


similar("angel", "angel")
similar("angel", "a n g e l")
similar("angel", "Angel")
similar("angel", "angelito")
similar("angel", "arcangel")
similar("angel", "angelopolis")
similar("angel", "carburador")

pivot = colonies[0]
for x in colonies[1:100]:
    similar(pivot, x)

n = 100
threshold = 0.6
for i in range(n):
    pivot = colonies[i]
    for j in range(i + 1, n):
        s = colonies[j]
        score = similar(pivot, s) 
        if score > threshold:
            print(score, idx[i], pivot, idx[j], s)