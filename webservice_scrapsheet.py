''' Created on November 4 2015 '''
#!/usr/bin/env python
# coding: utf-8
from __future__ import unicode_literals

import requests
import sqlite3

def make_dicts(cursor, row):
    return dict((cursor.description[idx][0], value)
                for idx, value in enumerate(row))

def get_db(sqlite_db):
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(sqlite_db)
        db.row_factory = make_dicts
    return db

def query_db(sqlite_db, query, args=(), one=False):
    cur = sqlite3.connect(sqlite_db).execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

def get_url_in_sqlite_relay(sqlite_db, base_id):
    query = 'SELECT url_webservice FROM retargeting_base_list WHERE id = ?'
    item = query_db(sqlite_db, query, [str(base_id)], one=True)
    if len(item) > 0:
        return item[0]
    else:
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

def test_criteo(base_id, nb):
    url = get_url_in_sqlite_relay('data_monetization.db', base_id)
    if url:
        print url
    else:
        print "Pas d'URL trouve"
        return False

    url = url.replace("criteo", "squadata")
    print url

    md5 ='21e945f025ecc8c03dfe63297d620108'
    md5_1 = '4430213659c9b665d5f14658b279dcaa'
    md5_2 = "778e4b024fb8084afdc585ed0adf13f1"
    md5_3 = "708f58b506853ab7236cddd522183f04"
    md5_4 = "a70c999e203b83f01619a075d1e0b55f"
    md5_list = [md5, md5_1, md5_2, md5_3, md5_4]
    #md5_list = [md5]

    md5_big_list = []
    for i in range(nb):
        md5_big_list.extend(md5_list)

    res = call_webservice(url, md5_list)
    for item in res:
        print item

def test_redirect(route, base_id, retargeter_id, domain = "prod.wizweb.tech"):
    url = "http://%s/%s" % (domain, route)
    print url
    feedback = requests.get(url)
    print feedback
    url = url + "/prod"
    if 'desabo' in route:
        url_data = "%s?base_id=%s&md5=test&rec=no" % (url, str(base_id))
    elif 'tag' in route:
        url_data = "%s?base_id=%s&id=%s&md5=test&rec=no" % (url, str(base_id), str(retargeter_id))
    elif 'data' in route:
        md5 = '4430213659c9b665d5f14658b279dcaa'
        md5_2 = "778e4b024fb8084afdc585ed0adf13f1"
        md5_3 = "708f58b506853ab7236cddd522183f04"
        md5_4 = "a70c999e203b83f01619a075d1e0b55f"
        url_data = "%s?base_id=%s&md5=%s&rec=no" % (url, str(base_id), md5)
    print url_data
    feedback = requests.get(url_data)
    print feedback
    if feedback.status_code == requests.codes.ok:
        try:
            res = feedback.json()
            print res
        except:
            pass
        try:
            res = feedback.content
            print res
        except:
            pass
#call_webservice(url, md5_big_list)

#test_criteo(4, 20)

"""
for route in ["tag", "desabo"]:
    test_redirect(route, 12, 5)
"""
#for route in ["data"]:
#    test_redirect(route, 5, 5)

test = "http://prod.wizweb.tech/data?base_id=12&id=5&md5=test"
redirect = {'base_id':12, 'id':5, 'md5':'test'}
url_redirect = "http://email-reflex.com/tags/pixel.php?h=mdr&source=393"
#feedback = requests.get(url, data = redirect)

url = "http://ws.capdecision.fr/wizweb/ws_wizweb.php?p1=c51f913a9c87a62369b09c762462a3f7&p2=8263d34fc9d23144dd0f9d22462290bb"
#call_webservice("http://ws.capdecision.fr/wizweb/ws_wizweb.php", ['c51f913a9c87a62369b09c762462a3f7', '8263d34fc9d23144dd0f9d22462290bb'])
#call_webservice("http://prod.abcmails.net/criteo/data/sho/", ['c51f913a9c87a62369b09c762462a3f7', '8263d34fc9d23144dd0f9d22462290bb'])
feedback = requests.get(url)
print feedback.text