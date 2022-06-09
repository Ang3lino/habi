#!/usr/bin/env python
# coding: utf-8

# In[1]:



import multiprocessing
import itertools
import pandas as pd
import os
import mysql.connector

from mysql.connector import Error
from tqdm import tqdm
from pprint import pprint
from multiprocessing import Process


# In[2]:


# %pip install mysql-connector-python


# In[3]:



# %pip install Unidecode nltk 


# In[ ]:



import nltk
import unidecode
import operator

from nltk.corpus import stopwords
from difflib import SequenceMatcher


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


# In[7]:




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


test_connection()
connection = get_conn()
cursor = connection.cursor()


def read_all(query, cursor=cursor):
    cursor.execute(query)
    return cursor.fetchall()

def print_all(query, cursor=cursor):
    records = read_all(query, cursor)
    pprint(records)


# In[8]:




# unique_states = [('Tamaulipas',), ('México',), ('Sinaloa',), ('Campeche',), ('DF / CDMX',), ('Coahuila de Zaragoza',), ('Chihuahua',), ('Durango',), ('Jalisco',), ('Colima',), ('Veracruz de Ignacio de la Llave',), ('Guanajuato',), ('Puebla',), ('Sonora',), ('Oaxaca',), (None,), ('Chiapas',), ('Quintana Roo',), ('Michoacán de Ocampo',), ('Baja California Sur',), ('Aguascalientes',), ('Baja California',), ('Yucatán',), ('Nayarit',), ('Guerrero',), ('Morelos',), ('Nuevo León',), ('Querétaro',), ('Hidalgo',), ('Tlaxcala',), ('Tabasco',), ('San Luis Potosí',), ('Zacatecas',), ('Distrito Federal',)]
unique_states = read_all("SELECT DISTINCT state FROM sepomex", cursor)
unique_states.sort()
result = read_all('DESC sepomex', cursor)
fields = [t[0] for t in result]


# In[10]:




# nltk.download("stopwords")
stopwords = stopwords.words('spanish')


# In[11]:



def download_csv_from_mysql(cursor, unique_states, fields):
    n = len(unique_states)
    pbar = tqdm(enumerate(unique_states), total=n)
    for i, state in pbar:
        state = state[0]
        query = f"SELECT {', '.join(fields)} FROM sepomex WHERE state = '{state}'"
        res = read_all(query, cursor)
        df = pd.DataFrame(data=res)
        df.columns = fields
        df.to_csv(f"csv/{i}.csv", index=False)
        pbar.set_description(state)


# download_csv_from_mysql(cursor, unique_states, fields)


# In[12]:



# Permanently changes the pandas settings
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


# In[13]:



def get_df_from_sources(partition_fname):
    cols = ['id', 'colony']
    df_sql = pd.read_csv(partition_fname)
    # print(df_sql)
    values_sql = df_sql[~df_sql['colony'].isnull()][cols].values

    state_label = df_sql.loc[0,'state']

    df_web = pd.read_csv("./CPdescarga.csv", sep="|", encoding='utf8')
    subset = df_web[df_web["d_estado"] == state_label]
    cols = ['d_codigo', 'd_asenta']
    values_web = subset[cols].values

    return values_sql, values_web


# In[32]:



def clean_str(s):
    s = s.lower()
    tokens = s.split(' ')
    new_s = [t for t in tokens if not t in stopwords]
    s = ' '.join(new_s)
    return unidecode.unidecode(s)

def get_data(values_sql, values_web):
    threshold = 0.8
    data = []  # idsql, idweb, new|update
    pbar = tqdm(values_sql, total=len(values_sql))
    for i, col_sql in pbar:
        sims = [(j, similar(col_sql, col_web), col_web) for j, col_web in values_web]
        sims.sort(key=operator.itemgetter(1), reverse=True)
        j, sim, col_web = sims[0]
        if sim == 0.0:  
            data.append([i, col_sql, "insert", sims[:5]])
        elif 1 > sim and sim > threshold:
            data.append([i, col_sql, "update", sims[:5]])
    return data


# In[15]:


# fnames


# In[26]:



def __load_colony_sims(fname):
    values_sql, values_web = get_df_from_sources("csv/" + fname)
    values_sql = [(i, clean_str(s)) for i, s in values_sql]
    values_web = [(i, clean_str(s)) for i, s in values_web]
    data = get_data(values_sql, values_web)
    df = pd.DataFrame(data, columns=['sql_id', 'colony_sql_name', 'action_string', 'knn_list'])
    df.to_csv("result/" + fname, index=False)

# fnames = os.listdir('csv')
def load_colony_sims(fnames, safe=True):
    n = len(fnames)
    for k, fname in enumerate(fnames):
        print("%d/%d in progress (%s)" % (k, n, fname))
        if safe:
            try:
                __load_colony_sims(fname)
            except Exception as e:
                print("[!] " + fname)
                print(e)
        else:
            __load_colony_sims(fname)


# In[17]:


# load_colony_sims(['2.csv'])


# In[44]:



def partitionate_source(src, cpu_count = multiprocessing.cpu_count()):
    # cpu_count = multiprocessing.cpu_count()
    split_count = len(src) // cpu_count
    groups = []
    for i in range(0, len(src), split_count):
        groups.append(fnames[i:i + split_count])
    return groups


fnames = os.listdir('csv')
# fnames = list(set(os.listdir('csv')) - set(os.listdir('result')))
fnames.sort()
groups = partitionate_source(fnames)
groups


# In[33]:



procs = [Process(target=load_colony_sims, args=(g,)) for g in groups]
for p in procs:
    p.start()
for p in procs:
    p.join()


# In[ ]:





# In[22]:



# print(similar("angel", "angel"))
# print(similar("angel", "a n g e l"))
# print(similar("angel", "Angel"))
# print(similar("angel", "angelito"))
# print(similar("angel", "arcangel"))
# print(similar("angel", "angelopolis"))
# print(similar("angel", "carburador"))


# In[27]:


# load_colony_sims(fnames)


# In[ ]:




