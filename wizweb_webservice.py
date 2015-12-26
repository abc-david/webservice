''' Created on October 10 2015 '''
#!/usr/bin/env python
# coding: utf-8
from __future__ import unicode_literals
__author__ = 'david'

import sys
import hashlib
import copy
import datetime
import psycopg2
import psycopg2.extensions
import psycopg2.pool
from psycopg2_prepare import PreparingCursor
from clean_data import *
import sqlite3
from threading import Thread
import json
import pandas as pd
import requests

from flask import request, Flask, redirect
app = Flask(__name__)
from flask import g

from contextlib import contextmanager

global default_url
default_url = 'http://188.165.197.228/desabo/feedback'

db_type = "postgres"
db_name = "prod" #"postgres"
db_user = "postgres"
db_host = "188.165.197.228"
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

# PostgreSQL functions
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
def getcursor(prepared = False):
    con = conn_pool.getconn()
    try:
        if prepared:
            yield con.cursor(cursor_factory=PreparingCursor)
        else:
            yield con.cursor()
    finally:
        conn_pool.putconn(con)

# Old code with HDF Storage
""" Using HDF Storage
def query_df(query):
    #initiate_threaded_connection_pool(db_package)
    with getconnection() as conn:
        df = pd.read_sql(query, conn)
    return df

def save_df(df, hdf_name):
    with pd.get_store('data_monetization' + '.h5') as hdf:
        hdf.put(hdf_name, df)

def get_df(hdf_name, query):
    with pd.get_store('data_monetization' + '.h5') as hdf:
        try:
            df = hdf[hdf_name]
        except:
            df = query_df(query)
            save_df(df, hdf_name)
    return df

def get_url(base_id, md5, hdf_name, query):
    df_url = get_df(hdf_name, query)
    try:
        df_slice = df_url.loc[df_url['base_id'] == int(base_id)]
        url = df_slice.iat[0, 1]
        if "[MD5]" in url:
            url = url.replace("[MD5]", md5)
        return url
    except:
        return False

def deal_with_url_redirection(base_id, md5, hdf_name = 'retargeting_desabo_url', \
                              query = "SELECT base_id, url FROM retargeting_desabo_action;"):
    url = get_url(base_id, md5, hdf_name, query)
    if not url:
        url = 'http://188.165.197.228/desabo/feedback'
    return url

def get_optin_id(base_id, hdf_name, query):
    df_base = get_df(hdf_name, query)
    try:
        df_slice = df_base[df_base['base_id'] == int(base_id)]
        optin_id = df_slice.iat[0, 1]
        return optin_id
    except:
        return False

def deal_with_internal_base(base_id, hdf_name = 'internal_list_correspondance', \
                            query = "SELECT id AS base_id, optin_id FROM retargeting_base_list;"):
    optin_id = get_optin_id(base_id, hdf_name, query)
    if optin_id:
        record_DB_optin_id(base_id, optin_id)

df_desabo_action = query_df("SELECT base_id, url FROM retargeting_desabo_action;")
#print df_desabo_action
save_df(df_desabo_action, 'retargeting_desabo_url')

df_base_correspondance = query_df("SELECT id AS base_id, optin_id FROM retargeting_base_list;")
#print df_base_correspondance
save_df(df_base_correspondance, 'internal_list_correspondance')
"""

# SQLite functions
def copy_table_in_sqlite(postgres_package, postgres_table, sqlite_db, sqlite_table = ""):
    if not 'conn_pool' in globals():
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

# Support functions
def divide_list_in_lists(input_list, number):
    list_list = []
    increment, left_over = divmod(len(input_list), number)
    start = 0
    end = increment
    for cpt in range(number):
        list_list.append(input_list[start:end])
        start += increment
        end += increment
    if left_over > 0:
        cpt_list = 0
        for item in input_list[-left_over:]:
            list_list[cpt_list].append(item)
            cpt_list += 1
    return list_list

def hash_mail_to_md5(string):
    string = string.lower().encode()
    hash_object = hashlib.md5(string)
    return hash_object.hexdigest()

# Desabo functions
def get_url_in_sqlite_desabo(sqlite_db, base_id, md5):
    query = 'SELECT url FROM retargeting_desabo_action WHERE base_id = ?'
    item = query_db(sqlite_db, query, [str(base_id)], one=True)
    if item:
        if item['url']:
            return item['url'].replace("[MD5]", md5)
    return default_url

def get_optin_id_in_sqlite(sqlite_db, base_id):
    'SELECT id AS base_id, optin_id FROM retargeting_base_list;'
    query = 'SELECT optin_id FROM retargeting_base_list WHERE id = ?'
    item = query_db(sqlite_db, query, [str(base_id)], one=True)
    if item:
        if item['optin_id']:
            return item['optin_id']
    return False

def record_DB_optin_id(rec, optin_id):
    #initiate_threaded_connection_pool(db_package)
    with getconnection() as conn:
        query_id = "SELECT m.mail_id FROM md5 AS m WHERE m.md5 = %s"
        cursor = conn.cursor()
        cursor.execute(query_id, (str(rec[1]), ))
        records = cursor.fetchone()
        if records:
            mail_id = int(str(records[0]).strip('()').strip(','))
            insert = "INSERT INTO optin_desabo (mail_id, optin_id, date, comment) VALUES (%s,%s,%s,%s)"
            conn.cursor().execute(insert, (str(mail_id), str(optin_id), rec[3], "data monetization"))
            conn.commit()
        else:
            return False

def deal_with_internal_base_sqlite(sqlite_db, base_id):
    optin_id = get_optin_id_in_sqlite(sqlite_db, base_id)
    if optin_id:
        record_DB_optin_id(base_id, optin_id)

def record_DB_desabo(rec):
    #initiate_threaded_connection_pool(db_package)
    args = [(str(item) if item else None) for item in rec]
    with getconnection() as conn:
        insert = "INSERT INTO retargeting_log_desabo (base_id, md5, mail, date) VALUES (%s,%s,%s,%s)"
        conn.cursor().execute(insert, tuple(args))
        conn.commit()

def extract_args_desabo(args):
    if 'base_id' in args:
        base_id = args['base_id']
    else:
        return [False]
    if 'mail' in args:
        mail = args['mail'].lower()
        md5 = hash_mail_to_md5(mail)
    elif 'md5' in args:
        md5 = args['md5']
        mail = None
    else:
        return [False]
    now = datetime.datetime.now().replace(microsecond=0).isoformat()
    record = [base_id, md5, mail, now]
    if 'rec' in args:
        if 'no' in args['rec'] or 'fa' in args['rec']:
            return [True, record, False]
    return [True, record, True]

def desabo_webservice(extract):
    if extract[0]:
        if extract[2]:
            record_thread = Thread_Return(target = record_DB_desabo, args = (extract[1], ))
            record_thread.start()
            deal_with_internal_base_sqlite('data_monetization.db', extract[1][1])
        url = get_url_in_sqlite_desabo('data_monetization.db', extract[1][0], extract[1][1])
        #return url
        return redirect(url)

# Redirect functions
def get_url_in_sqlite_redirect(sqlite_db, base_id, retargeter_id, md5):
    query = 'SELECT url FROM retargeting_cookie_url WHERE base_id = ? AND retargeter_id = ?'
    item = query_db(sqlite_db, query, [str(base_id), str(retargeter_id)], one=True)
    url = item['url'].replace("[MD5]", md5)
    return url

def record_DB_redirect(rec):
    #initiate_threaded_connection_pool(db_package)
    args = [(str(item) if item else None) for item in rec]
    with getconnection() as conn:
        insert = "INSERT INTO retargeting_log_cookie (base_id, retargeter_id, md5, mail, date) VALUES (%s,%s,%s,%s,%s)"
        conn.cursor().execute(insert, tuple(args))
        conn.commit()

def extract_args_redirect(args):
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
        mail = None
    else:
        return [False]
    now = datetime.datetime.now().replace(microsecond=0).isoformat()
    record = [base_id, retargeter_id, md5, mail, now]
    if 'rec' in args:
        if 'no' in args['rec'] or 'fa' in args['rec']:
            return [True, record, False]
    return [True, record, True]

def redirect_webservice(extract):
    if extract[0]:
        if extract[2]:
            record_thread = Thread_Return(target = record_DB_redirect, args = (extract[1], ))
            record_thread.start()
        url = get_url_in_sqlite_redirect('data_monetization.db', extract[1][0], extract[1][1], extract[1][2])
        #return url
        return redirect(url, code=302)
    else:
        return "Redirection Failed"

# Relay functions
global query_dict_relay
query_dict_relay = \
    {
    'log_md5' : "INSERT INTO retargeting_log_md5 (md5, mail, base_id, retargeter_id, date, status) " +
                "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
    'log_id_info' : "INSERT INTO retargeting_log_declarative_info (md5_id, prenom, nom, civilite, birth, cp, ville) " +
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
    'get_retargeter_id' : "SELECT id FROM retargeting_retargeter_list WHERE nom LIKE (%s)",
    }

def eval_list_and_prepare(entry_list, prepare_limit):
    prepare = False
    if isinstance(entry_list, list):
        if len(entry_list) > prepare_limit:
            prepare = True
        return [entry_list, prepare]
    else:
        return [[entry_list], prepare]

def cleanup_string(s):
    if s:
        if isinstance(s, (int,long)):
            return s
        else:
            try:
                s = s.encode('utf-8')
            except:
                pass
            if "'" in s:
                s = s.strip("'")
            return s
    else:
        return "None"

def clean_info_dict(item, dayfirst = True):
    cleaning_scripts = {'firstname' : clean_name, 'lastname' : clean_name, 'cp' : clean_cp, \
                    'ville' : clean_ville, 'title' : clean_civilite, 'birth' : clean_birth, \
                    'ip' : clean_ip, 'provenance' : clean_provenance, 'date' : clean_date, \
                    'port' : clean_port, 'tel' : clean_tel, 'tel1' : clean_tel, 'tel2' : clean_tel, 'fax' : clean_tel}
    new_item = {}
    for key, value in item.iteritems():
        if key in list(cleaning_scripts.iterkeys()):
            if cleaning_scripts[key] in ['clean_birth', 'clean_date']:
                new_item[key] = cleaning_scripts[key](str(value), dayfirst = dayfirst)
            else:
                new_item[key] = cleaning_scripts[key](str(value))
    return new_item

def get_url_in_sqlite_relay(sqlite_db, base_id):
    query = 'SELECT url_webservice FROM retargeting_base_list WHERE id = ?'
    item = query_db(sqlite_db, query, [str(base_id)], one=True)
    if item:
        if item['url_webservice']:
            return item['url_webservice']
    return False

def call_webservice(url, md5_list):
    payload = {}
    cpt = 1
    if not isinstance(md5_list, list):
        md5_list = [md5_list]
    for md5 in md5_list:
        payload['q' + str(cpt)] = md5
        cpt += 1
    feedback = requests.get(url, data = payload)
    if feedback.status_code == requests.codes.ok:
        res = feedback.json()
        return res
    else:
        return False

def force_base(res_list, default_base = 1):
    if res_list:
        if not isinstance(res_list, list):
            res_list = [res_list]
        for item in res_list:
            item['base'] = str(default_base)
        return res_list
    else:
        return False

def threaded_call_webservice(sqlite_db, base_id, md5_list, thread_limit = 1, thread_num = 8):
    url = get_url_in_sqlite_relay(sqlite_db, base_id)
    if url:
        if isinstance(md5_list, list):
            nb_md5 = len(md5_list)
            if nb_md5 > thread_limit:
                if thread_num > nb_md5:
                    thread_num = nb_md5
                md5_list_list = divide_list_in_lists(md5_list, thread_num)
                thread_list = []
                for cpt_thread in range(thread_num):
                    thread_list.append(Thread_Return(target = call_webservice, args = (url, md5_list_list[cpt_thread])))
                for cpt_thread in range(thread_num):
                    thread_list[cpt_thread].start()
                record_list = []
                for cpt_thread in range(thread_num):
                    partial_result = thread_list[cpt_thread].join()
                    if partial_result:
                        record_list.extend(partial_result)
                return record_list if record_list else False
            else:
                return call_webservice(url, md5_list)
        else:
            md5_list = [md5_list]
            return call_webservice(url, md5_list)
    else:
        return False

def execute_prepared_statement_relay(cursor):
    for query_name, query in query_dict_relay.iteritems():
        cursor.prepare(query)

def execute_query_relay(cursor, query_name, args, prepare, return_result = True):
    arg_tuple = tuple([arg if arg is None else str(arg) for arg in args])
    query = query_dict_relay[query_name]
    if len(args) == 1:
        cursor.execute(query, (str(args[0]), ))
    else:
        cursor.execute(query, arg_tuple)
    if return_result:
        records = cursor.fetchone()
        if not records:
            return False
        else:
            res_list = [cleanup_string(item) for item in records]
            return res_list

def record_in_db_relay(res_list, base_id, retargeter, prepare_limit = 5):
    if res_list:
        now = datetime.datetime.now().replace(microsecond=0).isoformat()
        res_list, prepare = eval_list_and_prepare(res_list, prepare_limit)
        with getconnection() as conn:
            if prepare:
                cursor = conn.cursor(cursor_factory=PreparingCursor)
                execute_prepared_statement_relay(cursor)
            else:
                cursor = conn.cursor()
            res = execute_query_relay(cursor, 'get_retargeter_id', [str(retargeter)], False, return_result=True)
            if res:
                retargeter_id = res[0]
            else:
                retargeter_id = ""
            for item in res_list:
                args = [(str(item[key]) if key in item else None) for key in ['md5', 'email']]
                args.extend([str(base_id), str(retargeter_id), now, str(item['status']) if 'status' in item else None])
                res = execute_query_relay(cursor, 'log_md5', args, prepare, return_result=True)
                if res:
                    md5_id = res[0]
                    new_item = clean_info_dict(item)
                    args = [md5_id]
                    args_keys = ['firstname', 'lastname', 'title', 'birth', 'cp', 'ville']
                    infos = []
                    for key in args_keys:
                        if key in new_item:
                            if new_item[key] == 'None':
                                infos.append(None)
                            else:
                                infos.append(str(new_item[key]))
                        else:
                            infos.append(None)
                    if infos.count(None) < len(infos):
                        args.extend(infos)
                        execute_query_relay(cursor, 'log_id_info', args, prepare)
            conn.commit()

def extract_args_relay(args):
    if 'base_id' in args:
        base_id = args['base_id']
    else:
        return [False]
    md5_list = [value for key, value in args.iteritems() if key not in ['base_id', 'rec']]
    record = [base_id, md5_list]
    if 'rec' in args:
        if 'no' in args['rec'] or 'fa' in args['rec']:
            return [True, record, False]
    return [True, record, True]

def relay_webservice(extract, retargeter, force_base = False, default_base = 1):
    if extract[0]:
        res = threaded_call_webservice('data_monetization.db', extract[1][0], extract[1][1])
        if extract[2]:
            record_thread = Thread_Return(target=record_in_db_relay, args=(res, extract[1][0], retargeter))
            record_thread.start()
        if force_base:
            res = force_base(res, default_base = 1)
        if res:
            return json.dumps(res, indent = 4)
        else:
            return "No URL to query for base_id : %s and retargter : %s" % (str(base_id), retargeter)


initiate_threaded_connection_pool(db_package)

copy_table_in_sqlite(db_package, 'retargeting_desabo_action', 'data_monetization.db')
copy_table_in_sqlite(db_package, 'retargeting_base_list', 'data_monetization.db')

copy_table_in_sqlite(db_package, 'retargeting_cookie_url', 'data_monetization.db')

"""
def desabo():
    extract = extract_args_desabo(request.args)


def redirect_cookie():
    extract = extract_args_redirect(request.args)

"""

md5 = '4430213659c9b665d5f14658b279dcaa'
md5_2 = "778e4b024fb8084afdc585ed0adf13f1"
md5_3 = "708f58b506853ab7236cddd522183f04"
md5_4 = "a70c999e203b83f01619a075d1e0b55f"
md5_list = [md5, md5_2, md5_3, md5_4]
retargeter = "criteo"

with app.app_context():
    base_id = 4
    print get_url_in_sqlite_relay('data_monetization.db', base_id)
    extract = [True, [base_id, md5_list], False]
    print relay_webservice(extract, retargeter, force_base = False, default_base = 1)
    #extract = extract_args_redirect(request.args)

    #print url
    #return redirect(url)



