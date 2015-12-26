''' Created on October 10 2015 '''
#!/usr/bin/env python
# coding: utf-8
from __future__ import unicode_literals
__author__ = 'david'

import sys
import hashlib
import datetime
import psycopg2
import psycopg2.extensions
import psycopg2.pool
import sqlite3
from threading import Thread
import pandas as pd

from flask import request, Flask
app = Flask(__name__)
from flask import g

from contextlib import contextmanager

db_name = "prod" #"postgres"
db_user = "postgres"
db_host = "localhost"
db_pass = "penny9690"
global db_package
db_package = [db_name, db_user, db_host, db_pass]

class Thread_Return(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs, Verbose)
        self._return = None
    def run(self):
        if self._Thread__target is not None:
            self._return = self._Thread__target(*self._Thread__args,
                                                **self._Thread__kwargs)
    def join(self):
        Thread.join(self)
        return self._return

def initiate_threaded_connection_pool(db_package):
    connect_token = "dbname='" + db_package[0] + "' user='" + db_package[1] + \
                    "' host='" + db_package[2] + "' password='" + db_package[3] + "'"
    try:
        global conn_pool
        conn_pool = psycopg2.pool.ThreadedConnectionPool(1,100, connect_token)
        message = "OK : Threaded connection pool established with DB."
        #print message
    except:
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print message

@contextmanager
def getconnection():
    con = conn_pool.getconn()
    try:
        yield con
    finally:
        conn_pool.putconn(con)

@contextmanager
def getcursor():
    con = conn_pool.getconn()
    try:
        yield con.cursor()
    finally:
        conn_pool.putconn(con)

# Old code with HDF Storage
""" Using HDF Storage
def query_df_url():
    initiate_threaded_connection_pool(db_package)
    with getconnection() as conn:
        query = "SELECT base_id, retargeter_id, url FROM retargeting_cookie_url;"
        df_url = pd.read_sql(query, conn)
    return df_url

def save_df_url(df_url):
    with pd.get_store('data_monetization' + '.h5') as hdf:
        hdf.put('retargeting_cookie_url', df_url)

def get_df_url():
    with pd.get_store('data_monetization' + '.h5') as hdf:
        try:
            df_url = hdf['retargeting_cookie_url']
        except:
            df_url = query_df_url()
            save_df_url(df_url)
    return df_url

def get_url(base_id, retargeter_id, md5):
    df_url = get_df_url()
    df_slice = df_url.loc[(df_url['base_id'] == str(base_id)) & (df_url['retargeter_id'] == str(retargeter_id))]
    url = df_slice.at[0, 'url']
    url = url.replace("[MD5]", md5)
    return url

# this function is not useful, because items will be recorded one-by-one
def record_DB(record_list):
    initiate_threaded_connection_pool(db_package)
    with getconnection() as conn:
        prepare_log = "PREPARE log AS INSERT INTO retargeting_log_cookie (base_id, retargeter_id, md5, mail, date) VALUES ($1,$2,$3,$4,$5);"
        with conn.cursor() as cur:
            cur.execute(prepare_log)
            for rec in record_list:
                cur.execute("EXECUTE log", (str(rec[0]), str(rec[1]), rec[2], rec[3], str(rec[4])))
            conn.commit()

@app.route('/data')
def redirect():
    extract = extract_args(request.args)
    if extract[0]:
        record_thread = Thread_Return(target = record_DB_one, args = (extract[1], ))
        record_thread.start()
        url_thread = Thread_Return(target = get_url, args = (extract[1][0], extract[1][1], extract[1][2]))
        url_thread.start()
        return redirect(url_thread.join(), code=302)
    else:
        pass
"""

def copy_table_in_sqlite(postgres_package, postgres_table, sqlite_db, sqlite_table = ""):
    initiate_threaded_connection_pool(postgres_package)
    with getconnection() as conn:
        df_url = pd.read_sql("SELECT * FROM %s;" % str(postgres_table), conn)
    if not sqlite_table:
        sqlite_table = postgres_table
    with sqlite3.connect(sqlite_db) as conn:
        df_url.to_sql(sqlite_table, conn, if_exists = 'replace', index = False)

def make_dicts(cursor, row):
    return dict((cursor.description[idx][0], value)
                for idx, value in enumerate(row))

def get_db(sqlite_db):
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(sqlite_db)
        db.row_factory = make_dicts
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def query_db(sqlite_db, query, args=(), one=False):
    cur = get_db(sqlite_db).execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

def get_url_in_sqlite(sqlite_db, base_id, retargeter_id, md5):
    query = 'SELECT url FROM retargeting_cookie_url WHERE base_id = ? AND retargeter_id = ?'
    item = query_db(sqlite_db, query, [str(base_id), str(retargeter_id)], one=True)
    url = item['url'].replace("[MD5]", md5)
    return url

def hash_mail_to_md5(string):
    string = string.lower().encode()
    hash_object = hashlib.md5(string)
    return hash_object.hexdigest()

def record_DB_one(rec):
    initiate_threaded_connection_pool(db_package)
    with getconnection() as conn:
        insert = "INSERT INTO retargeting_log_cookie (base_id, retargeter_id, md5, mail, date) VALUES ($1,$2,$3,$4,$5)"
        conn.cursor().execute(insert, (str(rec[0]), str(rec[1]), rec[2], rec[3], str(rec[4])))
        conn.commit()

def extract_args(args):
    if 'id' in args:
        retargeter_id = args['id']
    else:
        return [False]
    if 'base_id' in args:
        base_id = args['base_id']
    else:
        return [False]
    if 'mail' in args:
        mail = args['mail'].lower()
        md5 = hash_mail_to_md5(mail)
    elif 'md5' in args:
        md5 = args['md5']
        mail = ""
    else:
        return [False]
    now = datetime.datetime.now().replace(microsecond=0).isoformat()
    return [True, [base_id, retargeter_id, md5, mail, now]]

@app.route('/data')
def redirect():
    extract = extract_args(request.args)
    if extract[0]:
        record_thread = Thread_Return(target = record_DB_one, args = (extract[1], ))
        record_thread.start()
        url_thread = Thread_Return(target = get_url_in_sqlite, args = (extract[1][0], extract[1][1], extract[1][2]))
        url_thread.start()
        return redirect(url_thread.join(), code=302)
    else:
        pass

copy_table_in_sqlite(db_package, 'retargeting_cookie_url', 'data_monetization.db')


with app.app_context():
    print get_url_in_sqlite('data_monetization.db',12,5,"mdr")
