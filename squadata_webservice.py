''' Created on 16 december 2015 '''
#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
__author__ = 'david'

import sys
import copy
import datetime
import requests
from threading import Thread
import json
from psycopg2_prepare import PreparingCursor
from bs4 import BeautifulSoup

from clean_data import *

from contextlib import contextmanager

config_path = "/home/david/python"

"""
db_dict = {
    'db_type' = "mysql", #"postgres"
    'db_name' = "test", #"prod" #"postgres"
    'db_user' = "root", #"postgres"
    'db_host' = "localhost",
    'db_pass' = "penny", #"penny9690"
    }

global query_dict
query_dict = {
    'query_id' : "SELECT m.mail_id FROM md5 AS m WHERE m.md5 = %s",

    'query_optin' : "SELECT opt.id FROM optin_match AS opt " + \
                    "WHERE opt.mail_id = %s AND opt.optin_id IN " + \
                        "(SELECT opt_list.id FROM optin_list AS opt_list " + \
                        "WHERE opt_list.abreviation = %s)",

    'query_desabo' : "SELECT desabo.id FROM optin_desabo AS desabo " + \
                     "WHERE desabo.mail_id = %s AND desabo.optin_id IN " + \
                        "(SELECT opt_list.id FROM optin_list AS opt_list " + \
                        "WHERE opt_list.abreviation = %s)",

    'query_info' : "SELECT b.mail, id.civilite, id.prenom, id.nom FROM base AS b " + \
                     "INNER JOIN id_unik AS id ON b.id = id.mail_id " + \
                     "WHERE id.mail_id = %s",

    'query_log' : "INSERT INTO log_criteo (mail_id, optin_id, status, date) VALUES (%s,%s,%s,%s)",

    'query_log_id_list' : "SELECT id FROM optin_list WHERE abreviation = %s",

    'log_md5' : "INSERT INTO retargeting_log_md5 (md5, mail, base_id, retargeter_id, date, status) " +
                "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",

    'log_id_info' : "INSERT INTO retargeting_log_declarative_info (md5_id, prenom, nom, civilite, birth, cp, ville) " +
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
    }

global query_dict_internal
query_dict_internal = {
    'query_id' : "SELECT m.mail_id FROM md5 AS m WHERE m.md5 = %s",

    'query_optin' : "SELECT opt.id FROM optin_match AS opt " + \
                    "WHERE opt.mail_id = %s AND opt.optin_id = %s",

    'query_desabo' : "SELECT desabo.id FROM optin_desabo AS desabo " + \
                     "WHERE desabo.mail_id = %s AND desabo.optin_id = %s",

    'query_info' : "SELECT b.mail, id.civilite, id.prenom, id.nom FROM base AS b " + \
                     "INNER JOIN id_unik AS id ON b.id = id.mail_id " + \
                     "WHERE id.mail_id = %s",

    'query_log' : "INSERT INTO retargeting_log_webservice (retargeter_id, base_id, mail_id, md5, status, date) VALUES (%s,%s,%s,%s,%s,%s)",

    'log_md5' : "INSERT INTO retargeting_log_md5 (md5, mail, base_id, retargeter_id, date, status) " +
                "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",

    'log_id_info' : "INSERT INTO retargeting_log_declarative_info (md5_id, prenom, nom, civilite, birth, cp, ville) " +
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id"
    }
"""

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

# Database connection functions
def import_config(path = ""):
    global db_dict
    global query_dict_internal
    try:
        from config_webservice import db_dict, query_dict_internal
    except:
        sys.path.append(path)
        from config_webservice import db_dict, query_dict_internal

def initiate_threaded_connection_pool():
    global postgres
    if "post" in db_dict['db_type']:
        postgres = True
    else:
        postgres = False
    try:
        global conn_pool
        if postgres:
            import psycopg2
            import psycopg2.extensions
            import psycopg2.pool
            connect_token = "dbname='%s' user='%s' host='%s' password='%s'" \
                            % (db_dict['db_name'], db_dict['db_user'], db_dict['db_host'], db_dict['db_pass'])
            conn_pool = psycopg2.pool.ThreadedConnectionPool(1,100, connect_token)
        else:
            """
            # https://github.com/adyliu/mysql-connector-python.git
            from mysql.connector.pooling import MySQLConnectionPool
            conn_pool = MySQLConnectionPool(pool_name=None,
                                            pool_size=32,
                                            user = db_dict['db_user'], password = db_dict['db_pass'],
                                            database = db_dict['db_name'], host = db_dict['db_host'])
            """
            pass
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

@contextmanager
def getconnection_mysql():
    con = conn_pool.get_connection()
    try:
        yield con
    finally:
        con.close()

@contextmanager
def getcursor_mysql(prepared = False):
    con = conn_pool.get_connection()
    try:
        yield con.cursor(prepared = prepared)
    finally:
        con.close()

def check_connections(max_connections = 90):
    try:
        with getcursor() as cursor:
            cursor.execute("SELECT sum(numbackends) FROM pg_stat_database;")
            records = cursor.fetchall()
    except:
        return
    if records == []:
        pass
    else:
        print records
        raw_res = str(records[0]).strip('()').strip(',')
        try:
            n_connections = int(raw_res)
        except:
            n_connections = int(raw_res[:-1])
        print n_connections
        if n_connections >= max_connections:
            import os
            os.system("service postgresql restart")
        return n_connections

# Support functions
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

def cleanup_for_criteo(record_list):
    res_list = []
    for record in record_list:
        record.pop("mail_id", "None")
        clean_record = {}
        for key, value in record.iteritems():
            if value:
                if value == "None":
                    pass
                else:
                    clean_record[key] = value
                    if isinstance(value, basestring):
                        if "," in value:
                            clean_record[key] = list(value.split(","))[0]
                    if key == "title":
                        if str(value) == '1':
                            clean_record[key] = "Mr."
                        else:
                            clean_record[key] = "Mme"
        res_list.append(clean_record)
    return res_list

def eval_list_and_prepare(entry_list, prepare_limit):
    prepare = False
    if isinstance(entry_list, list):
        if len(entry_list) > prepare_limit:
            prepare = True
        return [entry_list, prepare]
    else:
        return [[entry_list], prepare]

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

# Query functions
def prepared_statement(query_name, query):
    cpt = 1
    while "%s" in query:
        query = query.replace("%s", "$" + str(cpt), 1)
        cpt += 1
    return "PREPARE %s AS %s" % (str(query_name), str(query))

def execute_prepared_statement(prepared_cursor, log = False):
    if log:
        for query in [value for value in list(query_dict_internal.itervalues()) if not 'log' in value]:
            prepared_cursor.prepare(query)
    else:
        for query in [value for value in list(query_dict_internal.itervalues()) if 'log' in value]:
            prepared_cursor.prepare(query)

def execute_query(cursor, query_name, args, prepare, return_result = True):
    arg_tuple = tuple([arg if arg is None else str(arg) for arg in args])
    query = query_dict_internal[query_name]
    if postgres:
        if len(args) == 1:
            cursor.execute(query, (str(args[0]), ))
        else:
            cursor.execute(query, arg_tuple)
    else:
        cursor.execute(query, arg_tuple)
    if return_result:
        records = cursor.fetchone()
        if not records:
            return False
        else:
            res_list = [cleanup_string(item) for item in records]
            return res_list

# Log webservice calls
def log_ws_call_md5(cursor, call_result, base_id, retargeter_id, prepare, now, query_name):
    args = [(str(call_result[key]) if key in call_result else None) for key in ['md5', 'email']]
    args.extend([str(base_id), str(retargeter_id), now, str(call_result['status']) if 'status' in call_result else None])
    res = execute_query(cursor, query_name, args, prepare, return_result=True)
    if res:
        md5_id = res[0]
        return md5_id
    else:
        return False

def log_ws_call_data(cursor, call_result, md5_id, prepare, now, query_name, query_args):
    new_item = clean_info_dict(call_result)
    args = [md5_id]
    infos = []
    for key in query_args:
        if key in new_item:
            if new_item[key] == 'None' or new_item[key] == '':
                infos.append(None)
            else:
                infos.append(str(new_item[key]))
        else:
            infos.append(None)
    if infos.count(None) < len(infos):
        args.extend(infos)
        execute_query(cursor, query_name, args, prepare)

def log_ws_call_per_list(cursor, res_list, base_id, retargeter_id, prepare,
                         query_name_md5 = 'log_md5', query_name_data = 'log_id_info',
                         query_args_data = ['firstname','lastname','title','birth','cp','ville']):
    now = datetime.datetime.now().replace(microsecond=0).isoformat()
    for item in res_list:
        res_log_md5 = log_ws_call_md5(cursor, item, base_id, retargeter_id, prepare, now, query_name_md5)
        if res_log_md5:
            md5_id = res_log_md5
            log_ws_call_data(cursor, item, md5_id, prepare, now, query_name_data, query_args_data)

def log_ws_call(res_list, base_id, retargeter_id, prepare_limit = 5):
    """ selects appropriate cursor (postgres/mysql, prepare on/off), then passes over arguments to log_ws_call_per_list """
    query_names = list(query_dict_internal.iterkeys())
    if "query_log" in query_names:
        if res_list:
            res_list, prepare = eval_list_and_prepare(res_list, prepare_limit)
            if postgres:
                with getconnection() as conn:
                    if prepare:
                        cursor = conn.cursor(cursor_factory = PreparingCursor)
                        execute_prepared_statement(cursor, log = True)
                    else:
                        cursor = conn.cursor()
                    log_ws_call_per_list(cursor, res_list, base_id, retargeter_id, prepare)
                    conn.commit()
            else:
                with getconnection_mysql() as conn:
                    cursor = conn.cursor(prepared = prepare)
                    log_ws_call_per_list(cursor, res_list, base_id, retargeter_id, prepare)
                    conn.commit()

# Internal webservice
def internals_query_from_md5(cursor, md5_list, optin_id, prepare = False):
    """ for each md5, queries DB to find out if md5 is known, is an optin, is a desabo, has declarative info.
    returns a list of dict, each dict containing the results for each md5 """
    result_list = []
    for md5 in md5_list:
        result = {}
        result['md5'] = md5
        res = execute_query(cursor, 'query_id', [md5], prepare)
        if not res:
            result['status'] = "NOMATCH"
        else:
            mail_id = int(res[0])
            result['mail_id'] = mail_id
            res = execute_query(cursor, 'query_optin', [mail_id, optin_id], prepare)
            if not res:
                result['status'] = "NOMATCH"
            else:
                res = execute_query(cursor, 'query_desabo', [mail_id, optin_id], prepare)
                if not res:
                    result['status'] = "OK"
                    result['base'] = '1'
                    res = execute_query(cursor, 'query_info', [mail_id], prepare)
                    if not res:
                        pass
                    else:
                        result['email'] = cleanup_string(res[0]) #res[0].strip("'")
                        result['title'] = cleanup_string(res[1]) #res[1].strip("'")
                        result['firstname'] = cleanup_string(res[2]) #res[2].strip("'")
                        result['lastname'] = cleanup_string(res[3]) #res[3].strip("'")
                else:
                    result['status'] = "OPTOUT"
        result_list.append(result)
    return result_list

def query_from_md5(md5_list, optin_id, prepare_limit = 5):
    """ selects appropriate cursor (postgres/mysql, prepare on/off), then passes over arguments to internals_query_from_md5 """
    md5_checked_list, prepare = eval_list_and_prepare(md5_list, prepare_limit)
    if postgres:
        if prepare:
            with getcursor(prepared = True) as prepared_cursor:
                execute_prepared_statement(prepared_cursor)
                return internals_query_from_md5(prepared_cursor, md5_checked_list, optin_id, prepare)
        else:
            with getcursor() as cursor:
                return internals_query_from_md5(cursor, md5_checked_list, optin_id, prepare)
    else:
        with getcursor_mysql(prepared = prepare) as cursor:
            return internals_query_from_md5(cursor, md5_checked_list, optin_id, prepare)

def threaded_query_from_md5(md5_list, optin_id,
                            thread_limit = 5, thread_num = 10, prepare_limit = 4):
    """ multi threads query_from_md5 : calculate number of threads depending on number of md5 """
    if isinstance(md5_list, list):
        nb_md5 = len(md5_list)
        if nb_md5 > thread_limit:
            if thread_num > nb_md5:
                thread_num = nb_md5
            md5_list_list = divide_list_in_lists(md5_list, thread_num)
            thread_list = []
            for cpt_thread in range(thread_num):
                thread_list.append(Thread_Return(target = query_from_md5,
                                                 args = (md5_list_list[cpt_thread], optin_id),
                                                 kwargs = {'prepare_limit' : prepare_limit}))
            for cpt_thread in range(thread_num):
                thread_list[cpt_thread].start()
            record_list = []
            for cpt_thread in range(thread_num):
                record_list.extend(thread_list[cpt_thread].join())
            #conn_pool.closeall()
            return record_list
        else:
            return query_from_md5(md5_list, optin_id, prepare_limit)
    else:
        md5_list = [md5_list]
        return query_from_md5(md5_list, optin_id, prepare_limit)

def internals_record_webservice_calls(cursor, record_list, base_id, retargeter_id, prepare = False):
    """ logs each record in appropriate table using 'query_log' query from 'query_dict_internal' """
    now = datetime.datetime.now().replace(microsecond=0).isoformat()
    for record in record_list:
        if 'mail_id' in record:
            if record['mail_id']:
                str_mail_id = str(record['mail_id'])
                str_md5 = None
        elif 'md5' in record:
            if record['md5']:
                str_md5 = str(record['md5'])
                str_mail_id = None
        args = [str(retargeter_id), str(base_id), str_mail_id, str_md5, str(record['status']), str(now)]
        execute_query(cursor, "query_log", args, prepare, return_result = False)

def record_webservice_calls(record_list, base_id, retargeter_id, prepare_limit = 5):
    """ selects appropriate cursor (postgres/mysql, prepare on/off), then passes over arguments to internals_record_webservice_calls """
    query_names = list(query_dict_internal.iterkeys())
    if "query_log" in query_names:
        record_checked_list, prepare = eval_list_and_prepare(record_list, prepare_limit)
        if postgres:
            with getconnection() as conn:
                if prepare:
                    cursor = conn.cursor(cursor_factory=PreparingCursor)
                    execute_prepared_statement(cursor, log = True)
                else:
                    cursor = conn.cursor()
                internals_record_webservice_calls(cursor, record_checked_list, base_id, retargeter_id, prepare)
                conn.commit()
            #conn_pool.closeall()
        else:
            with getconnection_mysql() as conn:
                cursor = conn.cursor(prepared = prepare)
                internals_record_webservice_calls(cursor, record_checked_list, base_id, retargeter_id, prepare)
                conn.commit()
    else:
        pass

def internal_webservice(md5_list, optin_id, retargeter_id, base_id, log = True,
                        thread_limit = 5, thread_num = 10, prepare_limit = 4):
    """ calls the DB with threaded_query_from_md5, and deals with logging & cleaning up the result """
    res_list = threaded_query_from_md5(md5_list, optin_id, thread_limit, thread_num, prepare_limit)
    if log:
        copy_res_list = copy.deepcopy(res_list)
        log_thread = Thread_Return(target = log_ws_call,
                                   args = (copy_res_list, base_id, retargeter_id))
        log_thread.start()
    res_list = cleanup_for_criteo(res_list)
    webservice_answer = json.dumps(res_list, indent = 4)
    return webservice_answer

# External webservice
def request_webservice(url, md5_list, param_key = 'q'):
    """ calls external webservice using the requests module """
    payload = {}
    cpt = 1
    if not isinstance(md5_list, list):
        md5_list = [md5_list]
    for md5 in md5_list:
        payload[param_key + str(cpt)] = md5
        cpt += 1
    feedback = requests.get(url, data = payload)
    if feedback.status_code == requests.codes.ok:
        res = feedback.json()
        return res
    else:
        return False

def force_base(res_list, default_base = 1):
    """ adds a 'base' property to each md5 record, whose value is equal to default_base (cf. criteo requirements) """
    if res_list:
        if not isinstance(res_list, list):
            res_list = [res_list]
        for item in res_list:
            item['base'] = str(default_base)
        return res_list
    else:
        return False

def threaded_request_webservice(url, md5_list, thread_limit, thread_num):
    """ multi threads request_webservice : calculate number of threads depending on number of md5 """
    if isinstance(md5_list, list):
        nb_md5 = len(md5_list)
        if nb_md5 > thread_limit:
            if thread_num > nb_md5:
                thread_num = nb_md5
            md5_list_list = divide_list_in_lists(md5_list, thread_num)
            thread_list = []
            for cpt_thread in range(thread_num):
                thread_list.append(Thread_Return(target = request_webservice,
                                                 args = (url, md5_list_list[cpt_thread])))
            for cpt_thread in range(thread_num):
                thread_list[cpt_thread].start()
            record_list = []
            for cpt_thread in range(thread_num):
                partial_result = thread_list[cpt_thread].join()
                if partial_result:
                    record_list.extend(partial_result)
            return record_list if record_list else False
        else:
            return request_webservice(url, md5_list)
    else:
        md5_list = [md5_list]
        return request_webservice(url, md5_list)

def external_webservice(url, md5_list, base_id, retargeter_id, log = True,
                        clean_up = False, force_base_bool = True, force_base_value = 1,
                        thread_limit = 10, thread_num = 5):
    """ calls the external webservice, and deals with logging & cleaning up the result """
    res_list = threaded_request_webservice(url, md5_list, thread_limit, thread_num)
    if res_list:
        try:
            res_list = json.loads(res_list)
        except:
            pass
        if log:
            copy_res_list = copy.deepcopy(res_list)
            log_thread = Thread_Return(target = log_ws_call,
                                       args = (copy_res_list, base_id, retargeter_id))
            log_thread.start()
        if clean_up: res_list = cleanup_for_criteo(res_list)
        if force_base_bool: res_list = force_base(res_list, force_base_value)
        webservice_answer = json.dumps(res_list, indent = 4)
        return webservice_answer
    else:
        return "External webservice does not respond. Pb with url : %s" % url

# XML webservice
def get_xml(base_xxx, db_package):
    #psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    #psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
    initiate_threaded_connection_pool(db_package)
    with getconnection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, nom FROM optin_list WHERE abreviation = %s", (str(base_xxx), ))
        records = cursor.fetchone()
        if records:
            optin_id = records[0]
            nom = records[1]
        else:
            optin_id = '0'
            nom = ""
        cursor.execute("SELECT xml FROM criteo_xml WHERE optin_id = %s AND usage = %s", (str(optin_id), 'header'))
        records = cursor.fetchone()[0]
        if records:
            header = records
        else:
            header = ""
        cursor.execute("SELECT xml FROM criteo_xml WHERE optin_id = %s AND usage = %s", (str(optin_id), 'footer'))
        records = cursor.fetchone()[0]
        if records:
            footer = records
        else:
            footer = ""
    conn_pool.closeall()
    post_dict = {}
    post_dict['id'] = '1'
    post_dict['nom'] = nom
    post_dict['header'] = header
    post_dict['footer'] = footer
    xml_doc = BeautifulSoup(features='xml')
    xml_doc.append(xml_doc.new_tag("bases"))
    xml_doc.bases.append(xml_doc.new_tag("base"))
    cpt_content = 0
    for key, value in post_dict.iteritems():
        xml_doc.bases.base.append(xml_doc.new_tag(str(key)))
        xml_container = xml_doc.bases.base.contents[cpt_content]
        if key == 'footer':
            xml_formatted_value = "<![CDATA[" + value + "]]>"
        else:
            xml_formatted_value = value
        xml_container.append(xml_doc.new_string(xml_formatted_value))
        cpt_content += 1
    xml_feed = xml_doc.prettify()
    xml_feed = xml_feed.replace("&lt;", "<").replace("&gt;", ">")#.replace("&lt;p&gt;", "").replace("&lt;/p&gt;", "")
    return xml_feed

# Webservice wrap-up
def extract_args(args, md5_only = False):
    if md5_only:
        md5_list = [value for key, value in args.iteritems() if key not in ['rec']]
        if md5_list:
            record = md5_list
        else:
            return [False]
    else:
        if 'bid' in args:
            base_id = args['bid']
        else:
            return [False]
        if 'rid' in args:
            retargeter_id = args['rid']
        else:
            return [False]
        md5_list = [value for key, value in args.iteritems() if key not in ['base_id', 'retargeter_id', 'rec']]
        if md5_list:
            record = [base_id, retargeter_id, md5_list]
        else:
            return [False]
    if 'rec' in args:
        if 'no' in args['rec'] or 'fa' in args['rec']:
            return [True, record, False]
    return [True, record, True]

def generic_webservice(extracted_args, md5_only = False, base_id = None, retargeter_id = None,
                       clean_up = False, force_base_bool = True, force_base_value = 1):
    if extracted_args[0]:
        if md5_only:
            md5_list = extracted_args[1]
        else:
            base_id = extracted_args[1][0]
            retargeter_id = extracted_args[1][1]
            md5_list = extracted_args[1][2]
        log = extracted_args[2]
        query_base_id = "SELECT optin_id, url_webservice FROM retargeting_base_list WHERE id = %s"
        with getcursor() as cursor:
            cursor.execute(query_base_id, (str(base_id), ))
            records = cursor.fetchone()
        if not records:
            return "No call made. Unable to match 'bid' value with an existing base.  (bid = %s)"  % (str(base_id))
        else:
            if records[0]:
                optin_id = records[0]
                print "internal with optin_id %s" % str(optin_id)
                return internal_webservice(md5_list, optin_id, retargeter_id, base_id, log,
                                           thread_limit = 5, thread_num = 10, prepare_limit = 4)
            else:
                if records[1]:
                    url = records[1]
                    print "external with url %s" % str(url)
                    return external_webservice(url, md5_list, base_id, retargeter_id, log,
                                               clean_up, force_base_bool, force_base_value,
                                               thread_limit = 10, thread_num = 5)
                else:
                    return "No call made. Unable to find a webservice URL for the specified base. (bid = %s)" % (str(base_id))
    else:
        return "No call made. Arguments missing in payload. Expected 'bid', 'rid', and md5 (list)."


import_config(config_path)
initiate_threaded_connection_pool()

retargeter_id = 1
md5 = '4430213659c9b665d5f14658b279dcaa'
base_id = 10 #12
print generic_webservice([True, [base_id,retargeter_id,md5], True])


""" Launch webservice
app = Flask(__name__)

@app.route('/criteo/', methods=['GET', 'POST'])
def index():
    return 'Flask is running'

@app.route('/criteo/data/you/', methods=['GET', 'POST'])
def data_you():
    md5_list = list(request.form.itervalues())
    #print md5_list
    json_res = criteo_webservice(md5_list, 'you', db_package)
    #print "OK json"
    return json_res

@app.route('/criteo/data/sho/', methods=['GET', 'POST'])
def data_sho():
    md5_list = list(request.form.itervalues())
    return criteo_webservice(md5_list, 'sho', db_package)

@app.route('/criteo/xml/you/', methods=['GET', 'POST'])
def xml_you():
    return get_xml('you', db_package)

@app.route('/criteo/xml/sho/', methods=['GET', 'POST'])
def xml_sho():
    return get_xml('sho', db_package)

app.run('0.0.0.0', threaded=True, port = 5000)

#if __name__ == '__main__':
#    app.run('0.0.0.0', threaded=True, port = 5100)
"""

""" test threaded
md5 = '4430213659c9b665d5f14658b279dcaa'
md5_2 = "778e4b024fb8084afdc585ed0adf13f1"
md5_3 = "708f58b506853ab7236cddd522183f04"
md5_4 = "a70c999e203b83f01619a075d1e0b55f"
md5_list = [md5, md5_2]
#for cpt in range(7):
    #md5_list.extend(md5_list)

from flask import request, Flask
app = Flask(__name__)

with app.app_context():

    print len(md5_list)
    print criteo_webservice(md5_list, 'sho')
"""

"""
res_list = threaded_query_from_md5(md5_list, db_package, base_xxx = 'you', thread_limit = 20, thread_num = 10, prepare_limit = 2)
#res_list = query_from_md5(md5_list, db_package, prepare_limit = 2)
print len(res_list)
print res_list
criteo_call_list = copy.deepcopy(res_list)
res_json = json.dumps(cleanup_for_criteo(res_list), indent = 4)
print res_list
record_criteo_calls(criteo_call_list, db_package, base_xxx = 'you')
check_connections()
#print res_json
"""
