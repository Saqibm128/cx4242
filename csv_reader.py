import pathlib
import pandas as pd
from pandas import DataFrame
import numpy as np

from scipy import stats
import time
import ntpath
from sqlalchemy import create_engine
import os


def create_connection():
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        engine = create_engine("mysql://admin:M0ZtDjLgAmj8D0LzAksT@cx4242.c55yrwcgiytm.us-east-1.rds.amazonaws.com/cx4242")
        conn = engine.connect()
        return conn
    except Exception as e:
        print(e)

    return conn

def update_db(conn, sql_command):
    """ create a table from the sql statement
    :param conn: Connection object
    :param create_table_sql: sql statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql_command)
        conn.commit()
    except Exception as e:
        print(e)

# citibike = """CREATE TABLE IF NOT EXISTS "citibike" (
# 					"index" INTEGER,
# 					  "tripduration" INTEGER,
# 					  "starttime" TEXT,
# 					  "stoptime" TEXT,
# 					  "start station id" INTEGER,
# 					  "start station name" TEXT,
# 					  "start station latitude" REAL,
# 					  "start station longitude" REAL,
# 					  "end station id" REAL,
# 					  "end station name" TEXT,
# 					  "end station latitude" REAL,
# 					  "end station longitude" REAL,
# 					  "bikeid" INTEGER,
# 					  "usertype" TEXT,
# 					  "birth year" REAL,
# 					  "gender" INTEGER
# 					);"""

conn = create_connection()

# if conn is not None:
#     update_db(conn, """DROP TABLE citibike;""")


#Go through all CitiBike files in the csv_files folder, create a df for each, and append that df to the database
basepath = '/home/ubuntu/cx4242'
for file in os.listdir(basepath):
    if os.path.isfile(os.path.join(basepath, file)) and file[-4:] == ".csv":
        print(file)
        df=pd.read_csv(basepath + "/" + file)
        df.to_sql('citibike', con = conn, if_exists = 'append')

conn.close()
