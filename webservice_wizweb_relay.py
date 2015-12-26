''' Created on November 4 2015 '''
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
import sqlite3
from threading import Thread
import json
from flask import request, Flask
from bs4 import BeautifulSoup
import pandas as pd
import requests
from clean_data import *

from flask import request, Flask
app = Flask(__name__)
from flask import g

from contextlib import contextmanager

db_name = "prod" #"postgres"
db_user = "postgres"
db_host = "188.165.197.228"
db_pass = "penny9690"
global db_package
db_package = [db_name, db_user, db_host, db_pass]
global postgres
postgres = True

retargeter = "criteo"
sqlite_db = 'data_monetization.db'

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

def get_url_in_sqlite(sqlite_db, base_id):
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
    url = get_url_in_sqlite(sqlite_db, base_id)
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

def prepared_statement(query_name, query):
    cpt = 1
    while "%s" in query:
        query = query.replace("%s", "$" + str(cpt), 1)
        cpt += 1
    return "PREPARE %s AS %s" % (str(query_name), str(query))

def execute_prepared_statement(cursor, query_dict):
    for query_name, query in query_dict.iteritems():
        cursor.execute(prepared_statement(query_name, query))

def execute_query(cursor, query_name, args, prepare, return_result = True):
    arg_tuple = tuple([arg if arg is None else str(arg) for arg in args])
    if postgres:
        if prepare:
            query = "EXECUTE %s (%s)" % (str(query_name), ",".join(['%s'] * len(args)))
        else:
            query = query_dict[query_name]
        if not prepare and len(args) == 1:
            cursor.execute(query, (str(args[0]), ))
        else:
            cursor.execute(query, arg_tuple)
    else:
        query = query_dict[query_name]
        cursor.execute(query, arg_tuple)
    if return_result:
        records = cursor.fetchone()
        if not records:
            return False
        else:
            res_list = [cleanup_string(item) for item in records]
            return res_list

query_dict = {
'log_md5' : "INSERT INTO retargeting_log_md5 (md5, mail, base_id, retargeter_id, date, status) " +
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
'log_id_info' : "INSERT INTO retargeting_log_declarative_info (md5_id, prenom, nom, civilite, birth, cp, ville) " +
                "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
'get_retargeter_id' : "SELECT id FROM retargeting_retargeter_list WHERE nom LIKE (%s)",
    }

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

def record_in_db(res_list, base_id, retargeter, prepare_limit = 5):
    if res_list:
        now = datetime.datetime.now().replace(microsecond=0).isoformat()
        res_list, prepare = eval_list_and_prepare(res_list, prepare_limit)
        with getconnection() as conn:
            cursor = conn.cursor()
            res = execute_query(cursor, 'get_retargeter_id', [str(retargeter)], False, return_result=True)
            if res:
                retargeter_id = res[0]
            else:
                retargeter_id = ""
            if prepare:
                execute_prepared_statement(cursor, query_dict)
            for item in res_list:
                args = [(str(item[key]) if key in item else None) for key in ['md5', 'email']]
                args.extend([str(base_id), str(retargeter_id), now, str(item['status']) if 'status' in item else None])
                res = execute_query(cursor, 'log_md5', args, prepare, return_result=True)
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
                        execute_query(cursor, 'log_id_info', args, prepare)
            conn.commit()

def relay_webservice_call(md5_list, base_id, retargeter, sqlite_db, force_base = False, default_base = 1):
    res = threaded_call_webservice(sqlite_db, base_id, md5_list)
    record_thread = Thread_Return(target=record_in_db, args=(res, base_id, retargeter))
    record_thread.start()
    if force_base:
        res = force_base(res, default_base = 1)
    return json.dumps(res, indent = 4)


#copy_table_in_sqlite(db_package, 'retargeting_base_list', 'data_monetization.db')
#initiate_threaded_connection_pool(db_package)

md5 = '4430213659c9b665d5f14658b279dcaa'
md5_2 = "778e4b024fb8084afdc585ed0adf13f1"
md5_3 = "708f58b506853ab7236cddd522183f04"
md5_4 = "a70c999e203b83f01619a075d1e0b55f"
md5_list = [md5, md5_2, md5_3, md5_4]

with app.app_context():
    base_id = 4
#    print relay_webservice_call(md5_list, base_id, retargeter, sqlite_db)
    """
    url = get_url_in_sqlite('data_monetization.db', base_id)
    print url
    #res = call_webservice(url, md5_list)
    res = threaded_call_webservice('data_monetization.db', base_id, md5_list)
    if res:
        print len(res)
        for item in res:
            print item
    else:
        print res
    initiate_threaded_connection_pool(db_package)
    record_in_db(res, base_id, retargeter, prepare_limit = 5)
    """

"""
test_url = 'http://prod.abcmails.net/criteo/xml/sho/'
#test_url =

retour_webservice = requests.get(test_url)
print retour_webservice.encoding
print retour_webservice.text

xml_tree = xml.fromstring(retour_webservice.content)
base = xml_tree[0]
for child in base:
   print child.tag, child.text
"""