''' Created on 4 mai 2015 '''
#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'david'

import sys
import collections
import copy
import datetime
import psycopg2
import psycopg2.extensions
import psycopg2.pool
from threading import Thread
import json
from flask import request, Flask
from bs4 import BeautifulSoup

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

def query_from_md5(md5_list, base_xxx = 'you', prepare_limit = 5):
    prepare = False
    if isinstance(md5_list, list):
        if len(md5_list) > prepare_limit:
            prepare = True
    else:
        md5_list = [md5_list]
    result_list = []
    query_id = "SELECT m.mail_id FROM md5 AS m WHERE m.md5 = %s"
    query_optin = "SELECT opt.id FROM optin_match AS opt WHERE opt.mail_id = %s " +\
                "AND opt.optin_id IN (SELECT opt_list.id FROM optin_list AS opt_list WHERE opt_list.abreviation = %s)"
    query_desabo = "SELECT desabo.id FROM optin_desabo AS desabo WHERE desabo.mail_id = %s " +\
                "AND desabo.optin_id IN (SELECT opt_list.id FROM optin_list AS opt_list WHERE opt_list.abreviation = %s)"
    query_info = "SELECT b.mail, id.civilite, id.prenom, id.nom, id.cp FROM base AS b " + \
                "INNER JOIN id_unik AS id ON b.id = id.mail_id WHERE id.mail_id = %s"
    result = {}
    #initiate_threaded_connection_pool(db_package)
    with getcursor() as cursor:
        if prepare:
            prepare_id = "PREPARE query_id AS SELECT m.mail_id FROM md5 AS m WHERE m.md5 = $1"
            cursor.execute(prepare_id)
            prepare_optin = "PREPARE query_optin AS SELECT opt.id FROM optin_match AS opt WHERE opt.mail_id = $1 " +\
                            "AND opt.optin_id IN (SELECT opt_list.id FROM optin_list AS opt_list WHERE opt_list.abreviation = $2)"
            cursor.execute(prepare_optin)
            prepare_desabo = "PREPARE query_desabo AS SELECT desabo.id FROM optin_desabo AS desabo WHERE desabo.mail_id = $1 " +\
                            "AND desabo.optin_id IN (SELECT opt_list.id FROM optin_list AS opt_list WHERE opt_list.abreviation = $2)"
            cursor.execute(prepare_desabo)
            prepare_info = "PREPARE query_info AS SELECT b.mail, id.civilite, id.prenom, id.nom, id.cp FROM base AS b " +\
                            "INNER JOIN id_unik AS id ON b.id = id.mail_id WHERE id.mail_id = $1"
            cursor.execute(prepare_info)
        for md5 in md5_list:
            result = {}
            result['md5'] = md5
            if prepare:
                cursor.execute("EXECUTE query_id (%s)", (str(md5),))
            else:
                cursor.execute(query_id, (str(md5),))
            records = cursor.fetchall()
            if records == []:
                result['status'] = "false" #"NOMATCH"
            else:
                mail_id = int(str(records[0]).strip('()').strip(','))
                result['mail_id'] = mail_id
                if prepare:
                    cursor.execute("EXECUTE query_optin (%s, %s)", (str(mail_id), str(base_xxx)))
                else:
                    cursor.execute(query_optin, (str(mail_id), str(base_xxx)))
                records = cursor.fetchall()
                if records == []:
                    result['status'] = "false" #"NOMATCH"
                else:
                    if prepare:
                        cursor.execute("EXECUTE query_desabo (%s, %s)", (str(mail_id), str(base_xxx)))
                    else:
                        cursor.execute(query_desabo, (str(mail_id), str(base_xxx)))
                    records = cursor.fetchall()
                    if records == []:
                        result['status'] = "true" #"OK"
                        #result['base'] = '1'
                        if prepare:
                            cursor.execute("EXECUTE query_info (%s)", (str(mail_id),))
                        else:
                            cursor.execute(query_info, (str(mail_id), ))
                        records = cursor.fetchall()
                        if records == []:
                            pass
                        else:
                            res_list = list(str(records[0]).strip('()').split(', '))
                            result['email'] = res_list[0].strip("'")
                            result['title'] = res_list[1].strip("'")
                            result['firstname'] = res_list[2].strip("'")
                            result['lastname'] = res_list[3].strip("'")
                            result['zipcode'] = res_list[4].strip("'")
                            result['phone'] = ""
                    else:
                        result['status'] = "false" #"OPTOUT"
            result_list.append(result)
    return result_list

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

def threaded_query_from_md5(md5_list, db_package, base_xxx = 'you', thread_limit = 20, thread_num = 8, prepare_limit = 5):
    initiate_threaded_connection_pool(db_package)
    if isinstance(md5_list, list):
        if len(md5_list) > thread_limit:
            md5_list_list = divide_list_in_lists(md5_list, thread_num)
            thread_list = []
            for cpt_thread in range(thread_num):
                thread_list.append(Thread_Return(target = query_from_md5, args = (md5_list_list[cpt_thread], ), \
                                                kwargs = {'base_xxx' : base_xxx, 'prepare_limit' : prepare_limit}))
            for cpt_thread in range(thread_num - 1, 0, -1):
                thread_list[cpt_thread].start()
            record_list = []
            for cpt_thread in range(thread_num - 1, 0, -1):
                record_list.extend(thread_list[cpt_thread].join())
            conn_pool.closeall()
            return record_list
        else:
            return query_from_md5(md5_list, base_xxx = base_xxx, prepare_limit = 20)
    else:
        md5_list = [md5_list]
        return query_from_md5(md5_list, base_xxx = base_xxx, prepare_limit = 20)

def cleanup_for_adlead(record_list):
    res_list = []
    for record in record_list:
        record.pop("mail_id", "None")
        res = {}
        res['status'] = record['status']
        record.pop("status", "None")
        data = {}
        for key, value in record.iteritems():
            data[key] = value
            if value:
                if "," in value:
                    data[key] = list(value.split(","))[0]
                if key == "title":
                    if value == '1':
                        data[key] = "Mr."
                    else:
                        data[key] = "Mme"
        res['data'] = data
        res_list.append(res)
    return res_list

def record_adlead_calls(record_list, db_package, base_xxx = 'you'):
    now = datetime.datetime.now().replace(microsecond=0).isoformat()
    prepare_insert = "PREPARE insert_adlead AS INSERT INTO log_adlead (mail_id, optin_id, status, date) VALUES ($1,$2,$3,$4);"
    initiate_threaded_connection_pool(db_package)
    with getconnection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM optin_list WHERE abreviation = %s", (str(base_xxx), ))
        records = cursor.fetchall()
        if records == []:
            optin_id = 0
        else:
            optin_id = int(str(records[0]).strip('()').strip(','))
        cursor.execute(prepare_insert)
        for record in record_list:
            if 'mail_id' in record:
                if record['mail_id']:
                    cursor.execute("EXECUTE insert_adlead (%s, %s, %s, %s)", \
                        (str(record['mail_id']), str(optin_id), str(record['status']), str(now)))
        conn.commit()
    conn_pool.closeall()

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

def adlead_webservice(md5_list, base_xxx, db_package, thread_limit = 20, thread_num = 8, prepare_limit = 5):
    res_list = threaded_query_from_md5(md5_list, db_package, base_xxx = base_xxx, \
                    thread_limit = thread_limit, thread_num = thread_num, prepare_limit = prepare_limit)
    adlead_call_list = copy.deepcopy(res_list)
    return_thread = Thread_Return(target = cleanup_for_adlead, args = (res_list, ))
    return_thread.start()
    record_thread = Thread_Return(target = record_adlead_calls, args = (adlead_call_list, db_package), \
                                  kwargs = {'base_xxx' : base_xxx})
    record_thread.start()
    webservice_answer = json.dumps(return_thread.join(), indent = 4)
    #print webservice_answer
    record_thread.join()
    check_connections()
    return webservice_answer

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
    post_dict = collections.OrderedDict()
    post_dict['id'] = '1'
    post_dict['nom'] = nom
    post_dict['header'] = header
    post_dict['footer'] = footer
    xml_doc = BeautifulSoup(features='xml')
    xml_doc.append(xml_doc.new_tag("bases"))
    xml_doc.bases.append(xml_doc.new_tag("base"))
    xml_doc.bases.base["id"] = 1
    cpt_content = 0
    for key, value in post_dict.iteritems():
        xml_doc.bases.base.append(xml_doc.new_tag(str(key)))
        xml_container = xml_doc.bases.base.contents[cpt_content]
        if key == 'footer' or key == 'header':
            xml_formatted_value = "<![CDATA[" + value + "]]>"
        else:
            xml_formatted_value = value
        xml_container.append(xml_doc.new_string(xml_formatted_value))
        cpt_content += 1
    xml_feed = xml_doc.prettify()
    #xml_feed = xml_doc.decode()
    xml_feed = xml_feed.replace("&lt;", "<").replace("&gt;", ">")#.replace("&lt;p&gt;", "").replace("&lt;/p&gt;", "")
    return xml_feed

print adlead_webservice(["21e945f025ecc8c03dfe63297d620108"], 'sho', db_package)
#print get_xml('sho', db_package)