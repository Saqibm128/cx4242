from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth
auth = HTTPBasicAuth()
import sqlalchemy
import pandas as pd
import numpy as np
import functools
from flask_cors import CORS


app = Flask(__name__)
CORS(app)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://admin:M0ZtDjLgAmj8D0LzAksT@cx4242.c55yrwcgiytm.us-east-1.rds.amazonaws.com/cx4242'
# db = SQLAlchemy(app)

def read_sql(stmt):
    """ create a table from the sql statement
    :param conn: Connection object
    :param create_table_sql: sql statement
    :return:
    """
    engine = sqlalchemy.create_engine("mysql://admin:M0ZtDjLgAmj8D0LzAksT@cx4242.c55yrwcgiytm.us-east-1.rds.amazonaws.com/cx4242")
    print(stmt)
    with engine.connect() as conn:   
        try:
            r = pd.read_sql(stmt, conn)
            return r

        except Exception as e:
            print(e)

@app.route("/health")
def health():
    return "OK"

def simple_jsonify_df(df):
    if df is None:
        return jsonify({"values": None, "index": None, "columns": None})
    index = df.index.tolist()
    columns = df.columns.tolist()
    values = df.to_numpy().tolist()
    return jsonify({"index":index, "columns":columns, "values":values})
    

from routing_methods import *

if __name__ == '__main__':
    app.run(host= '0.0.0.0',debug=True)

