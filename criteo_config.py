
#!/usr/bin/env python
# -*- coding: utf-8 -*-

db_type = "postgresql" #"mysql"
db_name = "postgres"
db_user = "postgres"
db_host = "localhost"
db_pass = "password"

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
    }


import PySQLPool
PySQLPool.getNewPool().maxActiveConnections = 50
my_con = PySQLPool.getNewConnection(db_user, db_pass, db_host, db_name)
global query_pool
query_pool = PySQLPool.getNewQuery(my_con)

from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector import connect
global pool
pool = MySQLConnectionPool(pool_name=None,
                    pool_size=50,
                    pool_reset_session=True,
                    user = db_user, password = db_pass, database = db_name, host = db_host)

@contextmanager
def getconnection_mysql(buffered = True, raw = True):
    con = pool.get_connection(buffered = buffered, raw = raw)
    try:
        yield con
    finally:
        con.close()

@contextmanager
def getcursor_mysql(prepared = False, buffered = True, raw = True):
    con = pool.get_connection(buffered = buffered, raw = raw)
    try:
        yield con.cursor(prepared = prepared)
    finally:
        con.close()

con = pool.get_connection()