''' Created on 8 june 2015 '''
#!/usr/bin/env python
# coding: utf-8
from __future__ import unicode_literals
__author__ = 'david'

# Script to test WebService developed for Criteo
"""
#test_url = 'http://prod.abcmails.net/criteo/xml/sho/'
test_url =

retour_webservice = requests.get(test_url)
print retour_webservice.encoding
print retour_webservice.text

xml_tree = xml.fromstring(retour_webservice.content)
base = xml_tree[0]
for child in base:
   print child.tag, child.text
"""

# Stuff not used for now
"""
from suds.sudsobject import asdict
import json
import unicodedata

def make_unicode(input):
    if type(input) != unicode:
        try:
            input =  input.decode('utf-8')
            return input
        except:
            try:
                input =  input.decode('latin-1')
                return input
            except:
                try:
                    input =  input.decode('utf-8', 'ignore')
                    return input
                except:
                    input =  input.decode('latin-1', 'ignore')
                    return input
    else:
        return input

def strip_accents(s):
   return ''.join(c for c in unicodedata.normalize('NFD', make_unicode(s))
                  if unicodedata.category(c) != 'Mn')

def my_strip_accents(s):
    accents_dict = {u'U+00E9' : 'e'}
    return ''.join(c for c in unicodedata.normalize('NFD', make_unicode(s))
                  if unicodedata.category(c) != 'Mn')

def strip_accents_from_dict(dict):
    new_dict = {}
    for k,v in dict.iteritems():
        if isinstance(k, basestring): new_k = strip_accents(k.encode('latin-1'))
        if isinstance(v, basestring): new_k = strip_accents(v.encode('latin-1'))
        new_dict[k] = v
    return new_dict

def recursive_asdict(d):
    #Convert Suds object into serializable format.
    out = {}
    for k, v in asdict(d).iteritems():
        if hasattr(v, '__keylist__'):
            out[k] = recursive_asdict(v)
        elif isinstance(v, list):
            out[k] = []
            for item in v:
                if hasattr(item, '__keylist__'):
                    out[k].append(recursive_asdict(item))
                else:
                    out[k].append(item)
        else:
            out[k] = v
    return out

def suds_to_json(data):
    return json.dumps(recursive_asdict(data))
"""

mbaz_dict = {'showroom' : {'Login' : 'user_MindbazWS_ShowroomStyliste',
                           'Password' : 'V2a4M3zw',
                           'IdSite' : '213'},
             'youpick': {'Login': 'user_MindbazWS_ABCYoupick',
                         'Password': 'uGcK788b',
                         'IdSite': '197'},
             'subscriber' : "http://webservice.mindbaz.com/SubscriberV2.asmx",
             'campaign' : "http://webservice.mindbaz.com/Campaign.asmx",
             'stats' : "http://webservice.mindbaz.com/statisticsV2.asmx",
             'target' : "http://webservice.mindbaz.com/Target.asmx",
             'config' : "http://webservice.mindbaz.com/Config.asmx"}

base = 'showroom'
import io, shutil
import collections
import random
import numpy as np
import pandas as pd
#pd.set_option('io.hdf.default_format','table')
import datetime as d
import DB_threaded_connection as DB
from suds.client import Client
from tidylib import tidy_document
from bs4 import BeautifulSoup
from tables import *

def show_df(df, nb = 5):
    print df.head(nb)
    print str(len(df.index)) + " lines."

def mbaz_client(mbaz_dict, base, action):
    client = Client(mbaz_dict[action] + "?WSDL")
    token = client.factory.create("MindbazAuthHeader")
    for key, value in mbaz_dict[base].iteritems():
        setattr(token, key, value)
    client.set_options(soapheaders=token)
    return client

#print mbaz_client(mbaz_dict, 'youpick', 'subscriber')

# Script to get stats of a campaign -- TO BE FIXED --
""" """
def segment_list(mbaz_dict, base, id_campaign):
    mbaz = mbaz_client(mbaz_dict, base, 'campaign')
    res_segment = mbaz.service.GetAllSegmentIds(id_campaign)
    segment_list = []
    for segment_id in res_segment.int:
        segment_list.append(segment_id)
    return segment_list

#print segment_list(mbaz_dict, 'youpick', 77)

def sent_stats(id_segment, mbaz_dict, base,
               stats_list = ["nbAddressSent", "nbOpeners", "nbClickers", "nbClickers_unsub", "nbClickers_edito"]):
    mbaz = mbaz_client(mbaz_dict, base, 'stats')
    res_stats = mbaz.service.GetSentStatistics2(id_segment, 0)
    print id_segment
    print res_stats
    if res_stats:
        stats_dict = {}
        for info in stats_list:
            stats_dict[info] = getattr(res_stats, info)
        return stats_dict
    else:
        return ""

def sent_stats_campaign(id_campaign, mbaz_dict, base,
                        stats_list = ["nbAddressSent", "nbOpeners", "nbClickers", "nbClickers_unsub", "nbClickers_edito"]):
    # Pb. : sent_stats() needs IdSent, however segment_list() returns IdSegments, so it doesn't work !
    segment_stats_list = []
    for id_segment in segment_list(mbaz_dict, base, id_campaign):
        this_segment_stats = sent_stats(id_segment, mbaz_dict, base, stats_list)
        if this_segment_stats:
            segment_stats_list.append(this_segment_stats)
    print segment_stats_list
    campaign_stats = {}
    for info in stats_list:
        for segment_stats in segment_stats_list:
            campaign_stats[info] += segment_stats[info]
    return campaign_stats


#mbaz = mbaz_client(mbaz_dict, 'campaign', 'showroom')
#print mbaz.service.GetCampaign(698)
#print mbaz.service.GetAllSegments(698)

#print segment_list(773, mbaz_dict, 'showroom')
#print sent_stats(1866, mbaz_dict, "showroom")
#print sent_stats_campaign(746, mbaz_dict, 'showroom')

#mbaz = mbaz_client(mbaz_dict, 'showroom', 'target')
#seg_stats = mbaz.service.GetAllSegments(773, 0)
#print seg_stats

#mbaz = mbaz_client(mbaz_dict, 'showroom', 'stats')
#seg_stats = mbaz.service.GetSentStatistics2(1058, 0)
#seg_stats_dict = suds_to_json(seg_stats)
#print seg_stats


def target_name_and_id(mbaz_dict, base, name_filter = "", target_location = "myTestsLocation"):
    mbaz = mbaz_client(mbaz_dict, base, 'target')
    #print mbaz
    target_list_res = mbaz.service.List(location = target_location,
                                    start = 0, limit = 10,
                                    sortField = 'idTarget', sortDir = 'ASC',
                                    nameFilter = name_filter)
    if target_list_res.total > 0:
        target_dict = {}
        targets = target_list_res.records.TargetListRecords
        for target in targets:
            #target_dict[strip_accents(target.name)] = target.idTarget
            target_dict[target.name] = target.idTarget
        return target_dict
    else:
        return ""

def id_bat_target(mbaz_dict, base, affil = "AdLead"):
    target_dict = target_name_and_id(mbaz_dict, base, affil[0:4])
    for target_name, id_target in target_dict.iteritems():
        if affil[0:4].lower() in target_name.lower():
            return id_target
        else:
            return ""

def config_name_and_id(mbaz_dict, base, name_filter = "", target_location = "allLocation"):
    mbaz = mbaz_client(mbaz_dict, base, 'config')
    #print mbaz
    config_list_res = mbaz.service.List(location = target_location,
                                    start = 0, limit = 10,
                                    sortField = 'idTarget', sortDir = 'ASC',
                                    nameFilter = name_filter)
    if config_list_res.total > 0:
        config_dict = {}
        configs = config_list_res.records.ConfigListRecords
        for config in configs:
            #target_dict[strip_accents(target.name)] = target.idTarget
            config_dict[config.name] = config.idConfig
        return config_dict
    else:
        return ""

def id_config(mbaz_dict, base, config_search = "defaut"):
    config_dict = config_name_and_id(mbaz_dict, base, config_search[0:4])
    for config_name, id_config in config_dict.iteritems():
        if config_search.lower() in config_name.lower():
            return id_config
        else:
            return ""

def create_campaign(mbaz_dict, base, campaign_params, html_msg, text_msg = "",
                    affil = "AdLead", config_search = "defaut",
                    default_params_folder = "/home/david/affil_html"):
    campaign_params_dict = {'name' : "Default_name_generated_by_Python",
                            'campMode' : "DYNAMIQUE",
                            'campType' : "DEDIE",
                            'subject' : "Default_subject_generated_by_Python",
                            'hasTxtMsg' : False,
                            'idConfig' : 1,
                            'responseAlias' : "Thomas",
                            'senderAlias' : "Default_sender_generated_by_Python",
                            'genSpeed' : 100,
                            'idTestTarget' : 21,
                            'useListUnsubscribe' : True,
                            'nhdActive' : True}

    mbaz_campaign_params = campaign_input_from_text_file('default_campaign', 'params', default_params_folder)

    id_bat = ""
    if campaign_params:
        if 'affiliation' in campaign_params:
            if campaign_params['affiliation']:
                id_bat = id_bat_target(mbaz_dict, base, campaign_params['affiliation'])
    if not id_bat:
        if affil:
            id_bat = id_bat_target(mbaz_dict, base, affil)
    if id_bat: mbaz_campaign_params['idTestTarget'] = id_bat

    id_configuration = ""
    if campaign_params:
        if 'configuration' in campaign_params:
            if campaign_params['configuration']:
                id_configuration = id_config(mbaz_dict, base, campaign_params['configuration'])
    if not id_configuration:
        if config_search:
            id_configuration = id_config(mbaz_dict, base, config_search)
    if id_configuration: mbaz_campaign_params['idConfig'] = id_configuration

    if campaign_params:
        if 'nom' in campaign_params:
            if campaign_params['nom']:
                mbaz_campaign_params['name'] = campaign_params['nom']
        if 'sender' in campaign_params:
            if campaign_params['sender']:
                mbaz_campaign_params['senderAlias'] = campaign_params['sender']
        if 'objet' in campaign_params:
            if campaign_params['objet']:
                mbaz_campaign_params['subject'] = campaign_params['objet']
        if 'vitesse' in campaign_params:
            if campaign_params['vitesse']:
                mbaz_campaign_params['genSpeed'] = campaign_params['vitesse']

    if html_msg:
        mbaz_html_msg = html_msg
    else:
        mbaz_html_msg = "Test html"

    mbaz_text_msg = ""
    if text_msg:
        mbaz_campaign_params['hasTxtMsg'] = True
        mbaz_text_msg = text_msg

    mbaz = mbaz_client(mbaz_dict, base, 'campaign')
    campaign_params = mbaz.factory.create('CampaignParameters')
    for key, value in mbaz_campaign_params.iteritems():
        setattr(campaign_params, key, value)
    if mbaz_text_msg:
        create_camp_res = mbaz.service.CreateCampaign(campParameters = mbaz_campaign_params,
                                                      htmlMsg = mbaz_html_msg,
                                                      txtMsg = mbaz_text_msg)
    else:
        create_camp_res = mbaz.service.CreateCampaign(campParameters = mbaz_campaign_params,
                                                      htmlMsg = mbaz_html_msg)
    return create_camp_res.idCampaign

def eval_input(input, string_list = ["'", '"'], decimal_list = [",", "."]):
    if input.isdigit():
        for x in decimal_list:
            if x in input:
                input_num = input.split(x)
                return float(int(input_num[0]) + float("0." + input_num[1]))
        return int(input)
    else:
        new_input = ""
        for x in string_list:
            if x in input:
                new_input = "".join([char for char in input if char != x])
        if not new_input: new_input = str(input)
        if new_input[0].isdigit():
            for x in decimal_list:
                if x in new_input:
                    input_num = new_input.split(x)
                    return float(int(input_num[0]) + float("0." + input_num[1]))
        check_boolean = new_input.replace(" ", "").lower()
        if check_boolean in ['true', 'false']:
            return check_boolean == 'true'
        return new_input

def campaign_input_from_text_file(file_name, sub_folder = "", remove_space = True,
                                  folder = "/home/david/affil_html", sep = ":", ignore = "#"):
    new_dict = {}
    if sub_folder:
        path = folder + "/" + sub_folder + "/" + file_name
    else:
        path = folder + "/" + file_name
    with io.open(path, mode = 'r', encoding = 'utf-8') as f:
        for line in f:
            listed_line = line.strip().split(sep) # split around the sep sign
            if len(listed_line) > 1: # we have the sep sign in there
                dict_info = listed_line[1].split(ignore) # split around the ignore sign
                if len(dict_info) > 1:
                    if remove_space:
                        new_dict[eval_input(listed_line[0].replace(" ", ""))] = eval_input(dict_info[0].replace(" ", ""))
                    else:
                        new_dict[eval_input(listed_line[0].replace(" ", ""))] = eval_input(dict_info[0].strip())
                    #new_dict[listed_line[0]] = dict_info[0]
                else:
                    if remove_space:
                        new_dict[eval_input(listed_line[0].replace(" ", ""))] = eval_input(listed_line[1].replace(" ", ""))
                    else:
                        new_dict[eval_input(listed_line[0].replace(" ", ""))] = eval_input(listed_line[1].strip())
                    #new_dict[listed_line[0]] = listed_line[1]
    return new_dict

def clean_campaign_input(base, file_name, folder = "/home/david/affil_html", sep = ":"):
    base_dict = {'showroom' : "Showroom Styliste"}
    cpn = campaign_input_from_text_file(file_name, "", False, folder, sep)
    if "par" in cpn['sender']:
        cpn['sender'] = cpn['sender'] + " " + base_dict[base]
    cpn['nom'] = cpn['affiliation'] + "_" + cpn['mois'] + "_" + cpn['annonceur']
    shutil.copy2(folder + "/" + file_name, folder + "/archives/" + cpn['nom'] + ".txt")
    #os.remove(file_folder + "/" + file_name)

    if cpn['dossier_html']:
        html_folder = cpn['dossier_html']
    else:
        html_folder = folder
    with io.open(html_folder + "/" + cpn['html'] + ".html", 'r', encoding = 'utf-8') as html:
        raw_html = html.read()
    html_dict = campaign_input_from_text_file("replace_in_html", "params", True, folder, sep)
    for affil_code, mbaz_code in html_dict.iteritems():
        raw_html = raw_html.replace(affil_code, mbaz_code)
    with io.open(folder + "/archives/" + cpn['nom'] + ".html", 'w', encoding = 'utf-8') as html:
        html.write(raw_html)
    #os.remove(html_folder + "/" + cpn['html'])

    return [cpn, raw_html]

def create_campaign_from_file(mbaz_dict, base, file_name):
    params_from_file = clean_campaign_input(base, file_name)
    id_camp = create_campaign(mbaz_dict, base, params_from_file[0], params_from_file[1])
    print id_camp

def track_urls(mbaz_dict, base, id_campaign):
    mbaz = mbaz_client(mbaz_dict, base, 'campaign')
    mbaz.service.TrackAll(id_campaign)

def spam_score(mbaz_dict, base, id_campaign):
    mbaz = mbaz_client(mbaz_dict, base, 'campaign')
    spam = mbaz.service.GetSpamScore(id_campaign, 1, 1000)
    print spam

# create_campaign_from_file(mbaz_dict, base, 'test_file')
# track_urls(mbaz_dict, base, 786)
# spam_score(mbaz_dict, base, 786)

def campaign_info(mbaz_dict, base, id_campaign):
    mbaz = mbaz_client(mbaz_dict, base, 'campaign')
    try:
        cpn_info = mbaz.service.GetCampaign(id_campaign)
        if cpn_info:
            cpn_params = cpn_info.parameters
            return [cpn_info.idCampaign, cpn_params.name, cpn_params.senderAlias,
                    cpn_params.subject, cpn_info.htmlMsg]
            return cpn_info
        else:
            return ""
    except:
        return ""

def campaign_html(mbaz_dict, base, id_campaign):
    cpn_stuff = campaign_info(mbaz_dict, base, id_campaign)
    if cpn_stuff:
        cpn_html = cpn_stuff[4]
        clean_html, error_html = tidy_document(cpn_html)
        return clean_html
    else:
        return ""

def pretty_campaign_html(mbaz_dict, base, id_campaign):
    html = campaign_html(mbaz_dict, base, id_campaign)
    if html:
        soup = BeautifulSoup()
        return soup.prettify()
    else:
        return ""

# Functions to extract text from a campaign

def clean_spaces_in_text(text):
    while "  " in text:
        text = text.replace("  ", " ")
    while '\n ' in text:
        text = text.replace('\n ', '\n')
    while 'n' in text:
        text = text.replace('n', '\n')
    while '\n\n' in text:
        text = text.replace('\n\n', '\n')
    return text

def find_between(s, first, last):
    try:
        if first:
            start = s.index(first) + len(first)
            end = s.index(last, start)
            return [True, s[:start-len(first)], s[start:end], s[end+len(last):]]
        else:
            end = s.index(last)
            return [True, s[:end], s[end:end+len(last)], s[end+len(last):]]
    except ValueError:
        return [False]

def erase_before(s, last):
    res_find = find_between(s, "", last)
    if res_find[0]:
        return res_find[3]
    else:
        return s

def erase_between_dict(s, cleanup_dict):
    for open, close in cleanup_dict.iteritems():
        while open in s:
            res_find = find_between(s, open, close)
            if res_find[0]:
                s = res_find[1] + " " + res_find[3]
            else:
                break
    return s

def bracket_cleaning(s):
    bracket = collections.OrderedDict
    bracket = {"\n{": "{", "\n}": "}", "{ ": "{", " }": "}"}
    for k, v in bracket.iteritems():
        while k in s:
            s = s.replace(k, v)
    return s

def deep_cleaning(s, cleanup = ""):
    if not cleanup:
        cleanup = []
        html_list = ['td', 'table', 'div', 'body', 'img', 'br', 'p', 'span', 'a', 'html', 'th', 'strong',
                     'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                     '.ReadMsgBody', '.ExternalClass', '.ExternalClass *', '#backgroundTable', '.image_fix']
        signals_list = [{'[': "]", ' [': "]", '{': "}", ' {': "}"},
                        {'[': ";\n}", '{': ";\n}"}]
        for signals in signals_list:
            for k, v in signals.iteritems():
                cleaning_dict = {}
                for tag in html_list:
                    cleaning_dict[tag + k] = v
                cleanup.append(cleaning_dict)
        dict_4 = {"{": "}", "/*": "*/"}
        cleanup.append(dict_4)
    if type(cleanup) is list:
        for cleanup_dict in cleanup:
            if "{" in s or "[" in s:
                s = erase_between_dict(s, cleanup_dict)
            else:
                break
    else:
        if "{" in s or "}" in s:
            s = erase_between_dict(s, cleanup)
    return s

def campaign_text(mbaz_dict, base, id_campaign, cleanup=True):
    html = campaign_html(mbaz_dict, base, id_campaign)
    if html:
        soup = BeautifulSoup(html)
        text = soup.get_text()
        text = clean_spaces_in_text(text)
        if cleanup:
            mirror_list = ["si vous ne visualisez pas correctement cet email suivez ce lien",
                           "visualiser ce message, veuillez cliquer-ici"]
            for item in mirror_list:
                if item in text:
                    text = erase_before(text, item)
            text = bracket_cleaning(text)
            if "@media only" in text:
                flag = "media_only"
                clean = collections.OrderedDict()
                clean = {'/*<': '>*/',
                         "@media only screen": "%}", "@media only": "}}", "@media on": "*/}",
                         'body{background': "}}", ".hide": "none}", "#outlook": ";}", ".External": ";}", "#back": ";}",
                         "* {": ";}"}
                clean_text = erase_between_dict(text, clean)
                if "@media only" in clean_text:
                    start = clean_text.find("@media only")
                    last = clean_text.rfind('}')
                    clean_text = clean_text[:start-len("@media only")] + " " + clean_text[last + 1:]
            else:
                flag = "autre cas"
                clean_2 = collections.OrderedDict()
                clean_2 = {'/*<': '>*/'}
                clean_text = erase_between_dict(text, clean_2)
            clean_text = deep_cleaning(clean_text)
            clean_text = clean_spaces_in_text(clean_text)
        if flag:
            return [clean_text, text, flag]
        else:
            return [text, text, ""]
    else:
        return ""

"""
# res = campaign_text(mbaz_dict, base, 94)
# print res

for i in range(0, 500):
    print str(i)
    res = campaign_text(mbaz_dict, base, i)
    if res:
        if True:
            if "{" in res[0]:
                print "*********", str(i), "*********", res[2]
                print res[1]
                print res[0]
"""

def campaign_business_info(mbaz_dict, base, id_campaign):
    mbaz = mbaz_client(mbaz_dict, base, 'campaign')
    return_list =[]
    try:
        cpn_info = mbaz.service.GetCampaign(id_campaign)
        #print cpn_info
        if cpn_info:
            cpn_name = cpn_info.parameters.name
            return_list.append(extract_affil_annonc(cpn_name))
            config_id = cpn_info.parameters.idConfig
            return_list.append(config_id)
            date = cpn_info.creationDate
            return_list.append(date)
            return return_list
        else:
            return ""
    except:
        return ""

def extract_affil_annonc(cpn_name):
    affil = ""
    annonc = ""
    for sep in ["_", "-"]:
        if sep in cpn_name:
            pos = cpn_name.find(sep)
            affil = cpn_name[:pos]
            ppos = cpn_name.rfind(sep, pos)
            annonc = cpn_name[ppos+len(sep):]
    if not annonc:
        annonc = cpn_name
    if "[" in annonc:
        res = find_between(annonc, "[", "]")
        if res:
            annonc = res[1] + res[3]
    return [affil, annonc]

def query_campagne_info(cursor, base, id_campaign):
    base_dict = {'showroom': 2, 'youpick': 4}
    select_query = "SELECT id, nom FROM campagne_list WHERE external_id = %s AND optin_id = %s"
    cursor.execute(select_query, (str(id_campaign), str(base_dict[base])))
    records = cursor.fetchone()
    if records:
        #print 'records =', records
        if records[1]:
            return [records[0], extract_affil_annonc(records[1])]
        else:
            return [records[0]]
    else:
        return False

def query_affil_annonc(connection, cursor, what, nom):
    table_dict = {'affil': 'campagne_affiliation', 'annonc': 'campagne_annonceur'}
    select_query = "SELECT id FROM " + table_dict[what] + " WHERE nom = %s"
    cursor.execute(select_query, (str(nom), ))
    records = cursor.fetchone()
    if records:
        return records[0]
    else:
        insert_query = "INSERT INTO " + table_dict[what] + " (nom) VALUES (%s) RETURNING id;"
        cursor.execute(insert_query, (str(nom), ))
        id = cursor.fetchone()[0]
        update_query = "UPDATE " + table_dict[what] + " SET group_id = %s WHERE id = %s"
        cursor.execute(update_query, (str(id), str(id)))
        connection.commit()
        return id

def update_campagne_info_in_db(connection, cursor, mbaz_dict, base, id_campaign):
    base_dict = {'showroom': 2, 'youpick': 4}
    routeur_dict = {'showroom': 2, 'youpick': 3}
    res_api = ""
    res = query_campagne_info(cursor, base, id_campaign)
    if res[0]:
        cpn_id = res[0]
        if res[1]:
            affil = res[1][0]
            annonc = res[1][1]
        else:
            res_api = campaign_business_info(mbaz_dict, base, id_campaign)
            if res_api:
                affil = res_api[0][0]
                annonc = res_api[0][1]
                config_id = res_api[1]
                cpn_date = res_api[2]
    try:
        print 'cpn_id =', cpn_id, "| affil =", affil, "| annonc =", annonc
    except:
        pass
    if affil:
        print 'affil = ' + affil
        affil_id = query_affil_annonc(connection, cursor, 'affil', affil)
        update_query = "UPDATE campagne_list SET affil_id = %s WHERE id = %s"
        cursor.execute(update_query, (str(affil_id), str(cpn_id)))
    if annonc:
        print 'annonc = ' + annonc
        annonc_id = query_affil_annonc(connection, cursor, 'annonc', annonc)
        update_query = "UPDATE campagne_list SET annonc_id = %s WHERE id = %s"
        cursor.execute(update_query, (str(annonc_id), str(cpn_id)))
    routeur_id = routeur_dict[base]
    update_query = "UPDATE campagne_list SET routeur_id = %s WHERE id = %s"
    cursor.execute(update_query, (str(routeur_id), str(cpn_id)))
    if not res_api:
        res_api = campaign_business_info(mbaz_dict, base, id_campaign)
        config_id = res_api[1]
        cpn_date = res_api[2]
    update_query = "UPDATE campagne_list SET config_id = %s WHERE id = %s"
    cursor.execute(update_query, (str(config_id), str(cpn_id)))
    update_query = "UPDATE campagne_list SET date = %s WHERE id = %s"
    cursor.execute(update_query, (cpn_date.strftime('%Y/%m/%d'), str(cpn_id)))
    connection.commit()

def populate_campagne_business_info(connection, cursor, mbaz_dict, base):
    optin_dict = {4: 'youpick', 2: 'showroom'}
    DB.initiate_threaded_connection_pool(DB.db_package)
    with DB.getconnection() as connection:
        cursor = connection.cursor()
        select_query = "SELECT external_id, optin_id FROM campagne_list WHERE id > 307"
        cursor.execute(select_query)
        records = cursor.fetchall()
        print records
        for record in records:
            external_id = record[0]
            if record[1] == 2:
                try:
                    base = optin_dict[record[1]]
                    print 'external_id =', external_id, '| base =', base
                    #info = campaign_business_info(mbaz_dict, base, external_id)
                    update_campagne_info_in_db(connection, cursor, mbaz_dict, base, external_id)
                    print "-------------------"
                except:
                    pass

#base = 'youpick'
#print campaign_business_info(mbaz_dict, base, 608)

def deal_with_tag_in_tag_list(connection, cursor, nom, genre, parent = ""):
    select_query = "SELECT id FROM tag_list WHERE nom = %s"
    cursor.execute(select_query, (str(nom).lower().strip("'"), ))
    records = cursor.fetchone()
    if records:
        return records[0]
    else:
        if parent:
            parent_id = deal_with_tag_in_tag_list(connection, cursor, parent, 'produit')
            insert_query = "INSERT INTO tag_list (nom, type, parent_id) VALUES (%s, %s, %s) RETURNING id;"
            cursor.execute(insert_query, (str(nom).lower().strip("'"), str(genre).lower().strip("'"), str(parent_id)))
            id = cursor.fetchone()[0]
        else:
            insert_query = "INSERT INTO tag_list (nom, type) VALUES (%s, %s) RETURNING id;"
            cursor.execute(insert_query, (str(nom).lower().strip("'"), str(genre).lower().strip("'")))
            id = cursor.fetchone()[0]
        connection.commit()
        return id

def deal_with_tag_in_tag_annonceur(connection, cursor, annonceur_id, nom, genre, parent = ""):
    tag_id = deal_with_tag_in_tag_list(connection, cursor, nom, genre, parent = parent)
    select_query = "SELECT id FROM tag_annonceur WHERE annonc_id = %s AND tag_id = %s"
    cursor.execute(select_query, (str(annonceur_id), str(tag_id)))
    if cursor.fetchone():
        pass
    else:
        insert_query = "INSERT INTO tag_annonceur (annonc_id, tag_id) VALUES (%s, %s)"
        cursor.execute(insert_query, (str(annonceur_id), str(tag_id)))
        connection.commit()

def populate_tag_annonceur_table(file_path):
    import collections
    DB.initiate_threaded_connection_pool(DB.db_package)
    df = pd.read_csv(file_path)
    cols = list(df)
    print cols
    for i in range(len(df.index)):
        record_dict = collections.OrderedDict()
        for col in cols:
            value = df.at[i, col]
            if str(value) != "nan":
                try:
                    record_dict[col.lower()] = int(value)
                except:
                    record_dict[col.lower()] = str(value).lower()
        annonc_id = record_dict['id']
        with DB.getconnection() as connection:
            cursor = connection.cursor()
            for genre, tag in record_dict.iteritems():
                pop_list = ['id', 'group_id', 'annonceur']
                if genre not in pop_list:
                    if genre == 'sous-produit':
                        tag_produit = record_dict['produit']
                        deal_with_tag_in_tag_annonceur(connection, cursor, annonc_id, tag, genre, tag_produit)
                    else:
                        deal_with_tag_in_tag_annonceur(connection, cursor, annonc_id, tag, genre)
            update_group_id = "UPDATE campagne_annonceur SET group_id = %s WHERE id = %s"
            cursor.execute(update_group_id, (str(record_dict['group_id']), str(annonc_id)))
            for i in range(3):
                record_dict.popitem(last=False)
            string_rep_list = []
            for k, v in record_dict.iteritems():
                string_rep_list.append("'"+str(k)+"': '"+str(v)+"'")
            string_rep = "{"+", ".join(string_rep_list)+"}"
            #import ast
            #back_to_dict = ast.literal_eval(string_rep)
            #print string_rep
            update_tag_comment = "UPDATE campagne_annonceur SET tag_comment = %s WHERE id = %s"
            cursor.execute(update_tag_comment, (str(string_rep), str(annonc_id)))
            connection.commit()

def truncate_table(table, connection):
    if table in ['campagne_tag', 'tag_ouvreur', 'base_ouvreur', 'base_injection_mindbaz']:
        truncate = "TRUNCATE TABLE %s RESTART IDENTITY;" % table
        cursor = connection.cursor()
        cursor.execute(truncate)
        connection.commit()

def populate_campagne_tag_from_join(start_cpn_id = 0, truncate = True):
    insert_from_join = "INSERT INTO campagne_tag (campagne_id, tag_id) " + \
                       "SELECT cl.id campagne_id, ta.tag_id FROM tag_annonceur ta " + \
                       "JOIN campagne_list cl ON cl.annonc_id = ta.annonc_id " + \
                       "WHERE cl.id > %s ORDER BY cl.id;"
    DB.initiate_threaded_connection_pool(DB.db_package)
    with DB.getconnection() as connection:
        if truncate:
            truncate_table('campagne_tag', connection)
        cursor = connection.cursor()
        cursor.execute(insert_from_join, (str(start_cpn_id), ))
        connection.commit()

def populate_campagne_tag_from_subject(start_cpn_id = 0):
    routeur_dict = {2:'showroom', 3:'youpick'}
    select_all_cpn = "SELECT id AS cpn_id, external_id, routeur_id FROM campagne_list " + \
                     "WHERE id > %s ORDER BY id;" % str(start_cpn_id)
    DB.initiate_threaded_connection_pool(DB.db_package)
    with DB.getconnection() as connection:
        cursor = connection.cursor()
        cpn = pd.read_sql(select_all_cpn, connection)
        cpn['sujet'] = ""
        for i in range(len(cpn.index)):
            try:
                cpn_id = cpn.at[i,'cpn_id']
                external_id = cpn.at[i,'external_id']
                routeur = routeur_dict[cpn.at[i,'routeur_id']]
                ask_api = True
            except:
                ask_api = False
            if ask_api:
                res_api = campaign_info(mbaz_dict, routeur, external_id)
                try:
                    cpn.at[i, "sujet"] = res_api[3]
                    print cpn_id, external_id, routeur, res_api[3]
                except:
                    pass
        cpn.to_csv("/home/david/python/divers/campagne_sujet.csv", encoding='utf-8')

def populate_tag_ouvreur(truncate = True):
    insert_tag_ouvreur = "INSERT INTO tag_ouvreur (mail_id, tag_id, score, last_date) " + \
                         "SELECT mail_id, tag_id, SUM(score) score, MAX(ouv_date) last_date FROM (" + \
                         "SELECT DISTINCT ON (ouv.mail_id, tag.tag_id, ouv.campagne_id) " + \
                         "ouv.mail_id, tag.tag_id, ouv.campagne_id, date(ouv.date) ouv_date, 1 score " + \
                         "FROM campagne_ouvreur ouv JOIN campagne_tag tag ON tag.campagne_id = ouv.campagne_id " + \
                         "ORDER BY ouv.mail_id, tag.tag_id, ouv.campagne_id DESC) " + \
                         "AS tag_query GROUP BY mail_id, tag_id;"
    DB.initiate_threaded_connection_pool(DB.db_package)
    with DB.getconnection() as connection:
        if truncate:
            truncate_table('tag_ouvreur', connection)
        cursor = connection.cursor()
        cursor.execute(insert_tag_ouvreur)
        connection.commit()

def update_tag_counts():
    DB.initiate_threaded_connection_pool(DB.db_package)
    with DB.getconnection() as connection:
        tag_count = "SELECT tag_id, COUNT(*) nb FROM tag_ouvreur " + \
                    "GROUP BY tag_id ORDER BY tag_id;"
        total = pd.read_sql(tag_count, connection)
        for interval in [30, 60, 90, 120]:
            tag_count = "SELECT tag_id, COUNT(*) nb_%sj FROM tag_ouvreur " % str(interval) + \
                        "WHERE last_date::date > (CURRENT_DATE - INTERVAL '%s day')::date " % str(interval) + \
                        "GROUP BY tag_id ORDER BY tag_id;"
            int = pd.read_sql(tag_count, connection)
            total = pd.merge(total, int, how='left')
        total = total.fillna(0)
        update = "UPDATE tag_list SET nb = %s, nb_30j = %s, nb_60j = %s, nb_90j = %s, nb_120j = %s WHERE id = %s"
        cursor = connection.cursor()
        cols = list(total)
        for i in range(len(total.index)):
            rec = {}
            for col in cols:
                raw = total.at[i, col]
                raw = str(raw)
                if "." in raw:
                    pos = raw.find(".")
                    raw = raw[:pos]
                rec[col] = raw
            cursor.execute(update, (rec['nb'], rec['nb_30j'], rec['nb_60j'], \
                            rec['nb_90j'], rec['nb_120j'], rec['tag_id']))
        connection.commit()

def populate_base_ouvreur(truncate = True, filter = True):
    if filter:
        insert = "INSERT INTO base_ouvreur (mail_id, nb_ouv, last_date, mail, domain) " + \
                 "SELECT co.mail_id, COUNT(DISTINCT co.campagne_id) nb_ouv, MAX(co.date::date) last_date, " + \
                 "ba.mail, ba.domain FROM campagne_ouvreur co JOIN base ba ON ba.id = co.mail_id WHERE " + \
                 "NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = ba.id) " + \
                 "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = ba.id) " + \
                 "AND NOT EXISTS (SELECT 1 FROM base_mimi_npai npai WHERE npai.mail_id = ba.id) " + \
                 "GROUP BY mail_id, ba.mail, ba.domain ORDER BY last_date DESC, nb_ouv DESC;"
    else:
        insert = "INSERT INTO base_ouvreur (mail_id, nb_ouv, last_date, mail, domain) " + \
                 "SELECT co.mail_id, COUNT(DISTINCT co.campagne_id) nb_ouv, MAX(co.date::date) last_date, " + \
                 "ba.mail, ba.domain FROM campagne_ouvreur co JOIN base ba ON ba.id = co.mail_id " + \
                 "GROUP BY mail_id, ba.mail, ba.domain ORDER BY last_date DESC, nb_ouv DESC;"
    DB.initiate_threaded_connection_pool(DB.db_package)
    with DB.getconnection() as connection:
        if truncate:
            truncate_table('base_ouvreur', connection)
        cursor = connection.cursor()
        cursor.execute(insert)
        connection.commit()

def populate_base_injection_mindbaz(path = "", truncate = True):
    if not path:
        path = "/home/david/python/divers/ExportMindbaz_candidats-ouvreurs_Sept2015.csv"
    df = pd.read_csv(path)
    from import_functions_OVH_DB import hash_mail_to_md5
    df['md5'] = df['mail'].apply(lambda x: hash_mail_to_md5(x))
    DB.initiate_threaded_connection_pool(DB.db_package)
    with DB.getconnection() as connection:
        if truncate:
            truncate_table('base_injection_mindbaz', connection)
        md5 = pd.read_sql("SELECT m.md5, m.mail_id, b.mail, b.domain FROM md5 m JOIN base b on b.id = m.mail_id", \
                          connection, coerce_float=False)
        merge = pd.merge(df, md5)[['mail_id', 'mail', 'domain']]
        merge = merge.sort('mail_id')
        merge.index = range(1, len(merge) + 1)
        prepare = "PREPARE insert AS INSERT INTO base_injection_mindbaz (mail_id, mail, domain) VALUES ($1, $2, $3)"
        cursor = connection.cursor()
        cursor.execute(prepare)
        for i in range(len(merge.index)):
            raw = merge.at[merge.index[i], 'mail_id']
            raw = str(raw)
            if "." in raw:
                pos = raw.find(".")
                raw = raw[:pos]
            cursor.execute("EXECUTE insert (%s, %s, %s)", (raw, merge.at[merge.index[i], 'mail'], \
                                                            merge.at[merge.index[i], 'domain']))
        connection.commit()

def count_list_candidates(cpn_id):
    #TODO: add restrictions on nb_days and nb_ouv
    list_pop = "SELECT tag.type, tag.nom, COUNT(DISTINCT ouv.mail_id) cpt FROM tag_ouvreur ouv " + \
               "JOIN tag_list tag ON tag.id = ouv.tag_id JOIN campagne_tag camp ON camp.tag_id = ouv.tag_id " + \
               "WHERE camp.campagne_id = %s GROUP BY tag.type, tag.nom" % str(cpn_id)
    DB.initiate_threaded_connection_pool(DB.db_package)
    with DB.getconnection() as connection:
        df = pd.read_sql(list_pop, connection)
        show_df(df)

#TODO: exclude plaintes from select queries
def get_list_tag(cpn_id, tag_type, nb_jour, nb_ouv, base):
    if base:
        desabo = "AND NOT EXISTS " + \
                 "(SELECT 1 FROM optin_desabo des JOIN optin_list list ON list.id = des.optin_id " + \
                 "WHERE des.mail_id = base.id AND list.abreviation = '%s') " % str(base)[:3]
    else:
        desabo = ""
    if tag_type:
        if isinstance(tag_type, basestring):
            restrict_tag = "AND tag.type = '%s' " % tag_type
        elif len(tag_type) > 1:
            list_dict = {}
            for tag in tag_type:
                list_dict[tag] = get_list_tag(cpn_id, tag, base)
            merge = list_dict[tag_type[0]]
            for tag in tag_type[1:]:
                merge = pd.merge(merge, list_dict[tag])
            return merge
        else:
            restrict_tag = "AND tag.type = '%s' " % tag_type[0]
    else:
        restrict_tag = ""
    if nb_ouv:
        restrict_ouv = "AND base.nb_ouv > '%s' " % str(nb_ouv)
    else:
        restrict_ouv = ""
    if nb_jour:
        restrict_jour = "AND base.last_date > (CURRENT_DATE - INTERVAL '%s day')::date " % str(nb_jour)
    else:
        restrict_jour = ""
    list_pop = "SELECT DISTINCT ON (base.mail_id) base.mail_id, base.nb_ouv, base.last_date, " + \
               "base.mail, base.domain FROM base_ouvreur base " + \
               "JOIN tag_ouvreur ouv ON ouv.mail_id = base.mail_id " + \
               "JOIN tag_list tag ON tag.id = ouv.tag_id " + \
               "JOIN campagne_tag camp ON camp.tag_id = ouv.tag_id " + \
               "WHERE camp.campagne_id = '%s' %s %s %s %s" % (str(cpn_id), restrict_tag, restrict_ouv,
                                                              restrict_jour, desabo) + \
               "GROUP BY base.mail_id, base.nb_ouv, base.last_date, base.mail, base.domain;"
    DB.initiate_threaded_connection_pool(DB.db_package)
    with DB.getconnection() as connection:
        list = pd.read_sql(list_pop, connection, coerce_float=False).sort(['last_date', 'nb_ouv'], ascending=[0, 0])
        list = list.set_index('mail_id', drop=False)
        #print cpn_id, tag_type, base
        #show_df(list)
        return list

def get_list_ouvreur(base, list_tag, nb_jour = 120, nb_ouv = 5):
    if base:
         desabo = "AND NOT EXISTS " + \
                  "(SELECT 1 FROM optin_desabo des JOIN optin_list list ON list.id = des.optin_id " + \
                  "WHERE des.mail_id = base.id AND list.abreviation = '%s') " % str(base)[:3]
    else:
        desabo = ""
    list_pop = "SELECT base.mail_id, base.nb_ouv, base.last_date, base.mail, base.domain FROM base_ouvreur base " + \
               "WHERE base.nb_ouv > '%s' " % str(nb_ouv) + \
               "AND base.last_date > (CURRENT_DATE - INTERVAL '%s day')::date %s" % (str(nb_jour), desabo) + \
               "ORDER BY last_date DESC, nb_ouv DESC;"
    DB.initiate_threaded_connection_pool(DB.db_package)
    with DB.getconnection() as connection:
        list = pd.read_sql(list_pop, connection, coerce_float=False).sort(['last_date', 'nb_ouv'], ascending=[0, 0])
        list = list.set_index('mail_id', drop=False)
    try:
        if len(list_tag.index) > 0:
            list_tag['exclude'] = True
            merged = pd.merge(list, list_tag, how='left')
            merged = merged[merged['exclude'] != True]
            merged = merged[['mail_id', 'nb_ouv', 'last_date', 'mail', 'domain']]
            merged = merged.set_index('mail_id', drop=False)
            return merged
    except:
        pass
    return list

def get_list_inactif(base, list_ouvreur, filter = True):
    if base:
         desabo = "AND NOT EXISTS " + \
                  "(SELECT 1 FROM optin_desabo des JOIN optin_list list ON list.id = des.optin_id " + \
                  "WHERE des.mail_id = b.mail_id AND list.abreviation = '%s') " % str(base)[:3]
    else:
        desabo = ""
    if filter:
        select = "SELECT b.mail_id, b.mail, b.domain FROM base_injection_mindbaz b WHERE " + \
                 "NOT EXISTS (SELECT 1 FROM campagne_ouvreur ouv WHERE ouv.mail_id = b.mail_id) " + \
                 "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.mail_id) " + \
                 "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.mail_id) %s" % desabo + \
                 "AND NOT EXISTS (SELECT 1 FROM base_mimi_npai npai WHERE npai.mail_id = b.mail_id);"
    else:
        select = "SELECT mail_id, mail, domain FROM base_injection_mindbaz b WHERE " + \
                 "NOT EXISTS (SELECT 1 FROM campagne_ouvreur ouv WHERE ouv.mail_id = b.mail_id) %s;" % desabo
    DB.initiate_threaded_connection_pool(DB.db_package)
    with DB.getconnection() as connection:
        list = pd.read_sql(select, connection, coerce_float = False).sort(['mail_id'], ascending=[1])
        list = list.set_index('mail_id', drop=False)
        list_ouvreur['exclude'] = True
        merge = pd.merge(list, list_ouvreur, how='left')
        merge = merge[merge['exclude'] != True]
        merge = merge[['mail_id', 'mail', 'domain']]
        merge = merge.set_index('mail_id', drop=False)
        print str(len(list) - len(merge))
        return merge

def tag_short_list(df, tag = 'test', size = 5000, method = 'best', nb_ouv = 7):
    return_list = pd.DataFrame()
    #print list(df)
    if len(list(df)) > 4:
        if size:
            if nb_ouv:
                r_list = df[df['nb_ouv'] > nb_ouv]
            else:
                r_list = df
            if size < len(r_list.index):
                if method in ['sample', 'random']:
                    return_list = r_list.ix[random.sample(r_list.index, size)]
                elif method == 'best':
                    nb_ouv_min = nb_ouv
                    while len(r_list.index) > size * 2:
                        nb_ouv_min += 1
                        r_list = r_list[r_list['nb_ouv'] > nb_ouv_min]
                    print nb_ouv_min
                    r_list = df[df['nb_ouv'] > (nb_ouv_min - 1)]

                    return_list = r_list[:size]
            elif size < len(df.index):
                if method in ['sample', 'random']:
                    return_list = df.ix[random.sample(df.index, size)]
                elif method == 'best':
                    nb_ouv_min = 1
                    r_list = df[df['nb_ouv'] > nb_ouv_min]
                    while len(r_list.index) > size * 1.2:
                        nb_ouv_min += 1
                        r_list = r_list[r_list['nb_ouv'] > nb_ouv_min]
                    r_list = df[df['nb_ouv'] > (nb_ouv_min - 1)]
                    return_list = r_list[:size]
        else:
            if nb_ouv:
                return_list = df[df['nb_ouv'] > nb_ouv]
            else:
                return_list = df
    else:
        # case for a list of inactifs, only three columns
        try:
            return_list = df.ix[random.sample(df.index, size)]
        except:
            return_list = df
    #show_df(return_list)
    df[tag] = ""
    selected_records = return_list.index
    df.loc[selected_records, tag] = True
    #show_df(df)
    try:
        #return [df, return_list['mail']]
        return df
    except:
        #return [df, pd.DataFrame(columns = ['mail_id'])]
        pass

def create_initial_lists(cpn_id, base, nb_jour = 120, nb_ouv = 5, test_size = 5000,
                        test_method = 'sample', test_nb_ouv = 7):
    #index = [tag.decode('utf-8').encode('utf-8') for tag in ['tag', 'open', 'inactif']]
    names = pd.DataFrame(index = ['tag', 'ouvreur', 'inactif'], columns= ['all', 'test'])
    #TODO: automatically define tag-select based on segments' sizes
    #TODO: rewrite test generation function as a schedule-based function
    tag_select = ['produit']
    tag_list = get_list_tag(cpn_id, tag_select, nb_jour, nb_ouv, base)
    tag_list = tag_short_list(tag_list, 'test', test_size, test_method, test_nb_ouv)
    names.at['tag', 'all'] = str(cpn_id) + "_tag[" + ",".join(tag_select) + "]_" + \
                            "open[last_open:" + str(nb_jour) + "j,min_ouv:" + str(nb_ouv) + "]_" + \
                            str(int(len(tag_list)/1000)) + "k"
    names.at['tag', 'test'] = "TEST_" + names.at['tag', 'all'] + \
                              "_[" + str(test_method) + ",min_ouv:" + str(test_nb_ouv) + "]_" + \
                              str(int(test_size)/1000) + "k"
    open_list = get_list_ouvreur(base, tag_list, nb_jour, nb_ouv)
    open_list = tag_short_list(open_list, 'test', test_size, test_method, test_nb_ouv)
    names.at['ouvreur', 'all'] = str(cpn_id) + \
                                 "_open[last_open:" + str(nb_jour) + "j,min_ouv:" + str(nb_ouv) + "]_" + \
                                 str(int(len(open_list)/1000)) + "k"
    names.at['ouvreur', 'test'] = "TEST_" + names.at['ouvreur', 'all'] + \
                                  "_test[" + str(test_method) + ",min_ouv:" + str(test_nb_ouv) + "]_" + \
                                  str(int(test_size)/1000) + "k"
    inactif_list = get_list_inactif(base, open_list)
    inactif_list = tag_short_list(inactif_list, 'test', test_size, test_method, test_nb_ouv)
    names.at['inactif', 'all'] = str(cpn_id) + "_inactif_" + str(int(len(inactif_list)/1000)) + "k"
    names.at['inactif', 'test'] = "TEST_" + names.at['inactif', 'all'] + \
                                  "_test[" + str(test_method) + ",min_ouv:" + str(test_nb_ouv) + "]_" + \
                                  str(int(test_size)/1000) + "k"
    tag_list = tag_list[['mail_id', 'nb_ouv', 'last_date', 'mail', 'domain', 'test']]
    #names = names.apply(lambda x: str(x).decode('utf-8').encode('utf-8'))
    with pd.get_store(base + '.h5') as hdf:
        hdf.put(str(cpn_id) + '/names', names)
        hdf.put(str(cpn_id) + '/lists/tag', tag_list)
        hdf.put(str(cpn_id) + '/lists/ouvreur', open_list)
        hdf.put(str(cpn_id) + '/lists/inactif', inactif_list)
        #print hdf

def create_date_range(nb_days_before = 10, nb_days_after = 20):
    d1 = d.datetime.now().date()
    date_list = []
    if nb_days_before:
        for i in xrange(nb_days_before, 0, -1):
            date_list.append(d1 - d.timedelta(days=i))
    if nb_days_after:
        for i in xrange(nb_days_after):
            date_list.append(d1 + d.timedelta(days=i))
    return date_list

def create_scheduler_df(nb_days_before, nb_days_after, base):
    select = "SELECT mail_id, mail, domain, 'ouvreur' AS status FROM base_ouvreur o UNION " + \
             "SELECT mail_id, mail, domain, 'inactif' AS status FROM base_injection_mindbaz i " + \
             "WHERE NOT EXISTS (SELECT 1 FROM base_ouvreur o WHERE o.mail_id = i.mail_id);"
    DB.initiate_threaded_connection_pool(DB.db_package)
    with DB.getconnection() as connection:
        df = pd.read_sql(select, connection)
        df.sort('mail_id', inplace = True)
        df = df.set_index('mail_id', drop=False)
        date_list = create_date_range(nb_days_before, nb_days_after)
        for date in date_list:
            #df[date.isoformat()] = np.nan
            df[date.isoformat()] = ""
    with pd.get_store(base + '.h5') as hdf:
        #hdf.put('scheduler', df.convert_objects(), encoding='utf-8')
        hdf.put('scheduler', df)

global idx
idx = [['max', 'sched', 'avail'], ['ouvreur', 'inactif'], ['orange', 'yahoo', 'other']]

global default_growth_sent_max
default_growth_sent_max = {'ouvreur': {'orange': 6, 'yahoo': 6, 'other': 6},
                           'inactif': {'orange': 0, 'yahoo': 0, 'other': 0}}

global default_seed_sent_max
default_seed_sent_max = {'ouvreur': {'orange': 15000, 'yahoo': 3000, 'other': 30000},
                         'inactif': {'orange': 0, 'yahoo': 0, 'other': 0}}

def create_sent_df(nb_days_before, nb_days_after, base):
    index = []
    for main in idx[0]:
        index.append(main)
        for dom in idx[2]:
            index.append(main + "_" + dom)
        for s in idx[1]:
            constructor = main + "_" + s
            index.append(constructor)
            for dom in idx[2]:
                index.append(constructor + "_" + dom)
    data = {}
    data['volume'] = index
    zero = [0] * len(index)
    cols = ['volume']
    for date in create_date_range(nb_days_before, nb_days_after):
        cols.append(date.isoformat())
        data[date.isoformat()] = zero
    df = pd.DataFrame(data = data, columns=cols)
    with pd.get_store(base + '.h5') as hdf:
        hdf.put('sent', df)

def eval_df_content(df_slice):
    if df_slice.empty:
        return [False]
    else:
        try:
            content = int(df_slice)
        except:
            try:
                content = str(df_slice)
            except:
                print df_slice
        if pd.isnull(content):
            return [False]
        elif content == "":
            return [False]
        else:
            try:
                content = int(content)
                return [True, content]
            except:
                print "PROBLEM", content

def eval_df_variable(df_var):
    if isinstance(df_var, pd.DataFrame):
        if not df_var.empty:
            return True
    return False

def update_sums_sent_df(purpose, date, sent_df, base):
    if eval_df_variable(sent_df):
        save_sent_df = False
        sent = sent_df
    else:
        save_sent_df = True
        with pd.get_store(base + '.h5') as hdf:
            sent = hdf['sent']

    recap = {}
    sdate = date.isoformat()
    for status in idx[1]:
        recap[status] = {}
        for domain in idx[2]:
            recap[status][domain] = int(sent.loc[sent['volume'] == "_".join([purpose, status, domain]), sdate])

    for status in idx[1]:
        sent.loc[sent['volume'] == "_".join([purpose, status]), sdate] = sum([recap[status][domain] for domain in idx[2]])
    for domain in idx[2]:
        sent.loc[sent['volume'] == "_".join([purpose, domain]), sdate] = sum([recap[status][domain] for status in idx[1]])
    sent.loc[sent['volume'] == purpose, sdate] = \
        sum([int(sent.loc[sent['volume'] == "_".join([purpose, status]), sdate]) for status in idx[1]])

    if save_sent_df:
        with pd.get_store(base + '.h5') as hdf:
            hdf.put('sent', sent)
    else:
        return sent


def fillup_sent_df_max(date, base, values = {}, growth = default_growth_sent_max, seed = default_seed_sent_max,
                       seed_date = d.datetime.now().date()):
    delta = date - seed_date
    default = {}
    if values:
        for status in values.iterkeys():
            default[status] = {}
            for domain in values[status].iterkeys():
                default[status][domain] = values[status][domain]
        for status in idx[1]:
            if status not in default.iterkeys():
                default[status] = {}
            for domain in idx[2]:
                if domain not in default[status].iterkeys():
                    default[status][domain] = 0
    else:
        for status in idx[1]:
            default[status] = {}
            for domain in idx[2]:
                default[status][domain] = int(seed[status][domain]*((1+(growth[status][domain]/100.0))**(delta.days)))

    sdate = date.isoformat()
    pdate = (date + d.timedelta(days=-1)).isoformat()
    with pd.get_store(base + '.h5') as hdf:
        sent  = hdf['sent']
        if sdate not in sent.columns:
                sent[sdate] = 0
        if pdate in sent.columns:
            for status in idx[1]:
                for domain in idx[2]:
                    ex = sent.loc[sent['volume'] == 'max_'+"_".join([status, domain]), pdate]
                    eval = eval_df_content(ex)
                    if values or not eval[0]:
                        sent.loc[sent['volume'] == 'max_'+"_".join([status, domain]), sdate] = default[status][domain]
                    else:
                        sent.loc[sent['volume'] == 'max_'+"_".join([status, domain]), sdate] = \
                            int(eval[1]*(1+(growth[status][domain]/100.0)))
        else:
            for status in idx[1]:
                for domain in idx[2]:
                    sent.loc[sent['volume'] == 'max_'+"_".join([status, domain]), sdate] = default[status][domain]

        sent = update_sums_sent_df('max', date, sent, base)

        hdf.put('sent', sent)

def update_sent_df(status, domain, date, sent_df, base):
    sdate = date.isoformat()
    query = {'orange': ["wanadoo|orange|voila", True],
             'yahoo': ["yahoo", True],
             'other': ["wanadoo|orange|voila|yahoo", False]}

    if eval_df_variable(sent_df):
        save_sent_df = False
        sent = sent_df
    else:
        save_sent_df = True
        with pd.get_store(base + '.h5') as hdf:
            sent = hdf['sent']

    with pd.get_store(base + '.h5') as hdf:
        sch = hdf['scheduler']

        if sdate not in sch.columns:
            sch[sdate] = 0
            hdf.put('scheduler', sch)
            return ""

        sched_avail_dict = {'sched': sch[sch[sdate] != ""],
                            'avail': sch[sch[sdate] == ""]}

        """
        for sched_avail, sch_slice in sched_avail_dict.iteritems():
            res_sched = {}
            for domain in idx[2]:
                index_constr = [sched_avail, domain]
                res_sched[domain] = len(sch_slice[sch_slice['domain'].str.contains(query[domain][0]) \
                                                     == query[domain][1]].index)
                sent.loc[sent['volume'] == "_".join([sched_avail, domain]), sdate] = res_sched[domain]
            sent.loc[sent['volume'] == "_".join([sched_avail, domain][:1]), sdate] = sum(res_sched.itervalues())
            for status in idx[1]:
                slice = sch_slice[sch_slice['status'] == status][['domain', sdate]]
                res_sched = {}
                for domain in idx[2]:
                    index_constr = [sched_avail, status, domain]
                    res_sched[domain] = len(slice[slice['domain'].str.contains(query[domain][0]) \
                                                  == query[domain][1]].index)
                    sent.loc[sent['volume'] == "_".join([sched_avail, status, domain]), sdate] = res_sched[domain]
                sent.loc[sent['volume'] == "_".join([sched_avail, status, domain][:2]), sdate] = sum(res_sched.itervalues())
        """

        for sched_avail, sch_slice in sched_avail_dict.iteritems():
            slice = sch_slice[sch_slice['status'] == status][['domain', sdate]]
            new_value = len(slice[slice['domain'].str.contains(query[domain][0]) == query[domain][1]].index)
            sent.loc[sent['volume'] == "_".join([sched_avail, status, domain]), sdate] = new_value

        for sched_avail in sched_avail_dict.iterkeys():
            sent = update_sums_sent_df(sched_avail, date, sent, base)

        if save_sent_df:
            with pd.get_store(base + '.h5') as hdf:
                hdf.put('sent', sent)
        else:
            return sent

def size_max_in_sent_df(cpn_id, status, date, base):
    lookup = {'tag': 'ouvreur', 'ouvreur': 'ouvreur', 'inactif': 'inactif'}
    sdate = date.isoformat()
    with pd.get_store(base + '.h5') as hdf:
        sent = hdf['sent']
        #print sent
        limit = {}
        for purpose in idx[0]:
            limit[purpose] = {}
            for domain in idx[2]:
                limit[purpose][domain] = int(sent.loc[sent['volume'] == "_".join([purpose, lookup[status], domain]), \
                                                      sdate])
        res = {}
        res['sending_cap'] = {}
        res['size_max'] = {}
        for domain in idx[2]:
            res['sending_cap'][domain] = limit['max'][domain] - limit['sched'][domain]
            res['size_max'][domain] = min(int(res['sending_cap'][domain]), int(limit['avail'][domain]))
    return res['size_max']

global size_max_cpn_per_day
size_max_cpn_per_day = {'tag': {'orange': 15000, 'yahoo': 3000, 'other': 12000},
                        'ouvreur': {'orange': 7500, 'yahoo': 1500, 'other': 6000},
                        'inactif': {'orange': 3000, 'yahoo': 500, 'other': 2500}}

def size_max_cpn(cpn_id, status, date, size_cpn = size_max_cpn_per_day):
    if size_cpn:
        return size_cpn[status]
    else:
        #TODO: get the info from the DB
        pass

def size_max(cpn_id, status, date, base, size_cpn = size_max_cpn_per_day):
    #TODO: check in DB if cpn still running for this date
    s_max_cpn = size_max_cpn(cpn_id, status, date, size_cpn)
    s_max_sent = size_max_in_sent_df(cpn_id, status, date, base)
    s_max = {}
    for domain in size_cpn[status].iterkeys():
        s_max[domain] = min(int(s_max_cpn[domain]), int(s_max_sent[domain]))
    return s_max

def schedule(cpn_id, status, domain, date, base, size):
    query = {'orange': ["wanadoo|orange|voila", True],
             'yahoo': ["yahoo", True],
             'other': ["wanadoo|orange|voila|yahoo", False]}
    lookup = {'tag': 'ouvreur', 'ouvreur': 'ouvreur', 'inactif': 'inactif'}
    sdate = date.isoformat()
    with pd.get_store(base + '.h5') as hdf:
        sch = hdf['scheduler']
        if sdate not in sch.columns:
            sch[sdate] = ""
        avail_sch = sch[(sch['status'] == lookup[status]) & (sch[sdate] == "")]
        avail_sch = avail_sch[avail_sch['domain'].str.contains(query[domain][0]) == query[domain][1]]
        #show_df(avail_sch)

        list = hdf[str(cpn_id) + "/lists/" + status]
        if 'scheduled' not in list.columns:
            list['scheduled'] = ""
        avail_list = list[list['scheduled'] != True]
        avail_list = avail_list[avail_list['domain'].str.contains(query[domain][0]) == query[domain][1]]
        #show_df(avail_list)

        merge = pd.merge(avail_list[['mail_id']], avail_sch[['mail_id']])
        merge = merge.set_index('mail_id', drop=False)
        #show_df(merge)
        print "Merge size for %s | %s | %s : %s" % (str(cpn_id), status, domain, str(len(merge)))
        print "SIZE size for %s | %s | %s : %s" % (str(cpn_id), status, domain, str(size))

        if size < len(merge):
            merge = merge.ix[random.sample(merge.index, size)]

        if len(merge) > 0:
            target = merge.index
            #print len(target)
            #print sdate
            list.loc[target, 'scheduled'] = True
            if sdate not in list.columns:
                list[sdate] = ""
            print "Schedule size for %s | %s | %s : %s" % (str(cpn_id), status, domain, str(len(merge)))
            list.loc[target, sdate] = True
            show_df(list.loc[list[sdate] == True][['mail_id', sdate]], 10)
            hdf.put(str(cpn_id) + "/lists/" + status, list)
            sch.loc[target, sdate] = str(cpn_id)
            show_df(sch[sch[sdate] == str(cpn_id)][['mail_id', sdate]], 10)
            hdf.put('scheduler', sch)

        update_sent_df(status, domain, date, False, base)

def schedule_status(cpn_id, status, date, base, size_status):
    for domain in size_status.iterkeys():
        schedule(cpn_id, status, domain, date, base, size_status[domain])

def schedule_date(cpn_id, date, base, size_cpn = size_max_cpn_per_day):
    for status in size_cpn.iterkeys():
        size_date = size_max(cpn_id, status, date, base, size_cpn)
        print status, size_date
        schedule_status(cpn_id, status, date, base, size_date)

def remove_node_hdf(node_list):
    with pd.get_store(base + '.h5') as hdf:
        if node_list:
            if type(node_list) == list:
                for node in node_list:
                    try:
                        hdf.get_node(node)._f_remove(recursive=True, force=True)
                    except:
                        pass
            else:
                try:
                    hdf.get_node(node_list)._f_remove(recursive=True, force=True)
                except:
                    pass
        else:
            pass


base = 'showroom'
#populate_base_ouvreur()
#populate_base_injection_mindbaz()
#populate_tag_annonceur_table("/home/david/python/divers/campagnes_tags.csv")
#populate_campagne_tag_from_join(0, False)
#populate_campagne_tag_from_subject(0)
#populate_tag_ouvreur(True, True)
#update_tag_counts()
#count_list_candidates(100)
#s = get_list_inactif(base = "showroom")
#s = get_list_tag(100, ['sous-produit', 'sexe'], 'showroom')
#s_lists = tag_short_list(s)
#cpn_dict = get_lists(100, 'showroom')
#store_lists(100, cpn_dict)

remove_node_hdf(['scheduler', 'sent', 'names', '100'])

d1 = d.datetime.now().date()
create_sent_df(0, 2, base)
fillup_sent_df_max(d1, base, {'ouvreur':{'orange':30000, 'yahoo':3000, 'other':30000}})
fillup_sent_df_max(d1 + d.timedelta(days=1), base)
fillup_sent_df_max(d1 + d.timedelta(days=2), base)
#fillup_sent_df_max(d1 + d.timedelta(days=3), base, {'ouvreur':{'orange':30000}}, seed_date=d1 + d.timedelta(days=-10))
#fillup_sent_df_max(d1 + d.timedelta(days=4), base)

create_scheduler_df(0, 2, base)
create_initial_lists(100, base)

for i in xrange(1):
    pass
#schedule(100, 'tag', 'yahoo', d1 + d.timedelta(days=0), base, 20000)
#schedule_status(100, 'tag', d1+ d.timedelta(days=0), base, size_max_cpn_per_day['tag'])
schedule_date(100, d1 + d.timedelta(days=0), base)

with pd.get_store(base + '.h5') as hdf:
    #pass
    #hdf.get_node('/100')._f_remove(recursive=True, force=True)
    #hdf.get_node('/sent')._f_remove(recursive=True, force=True)
    print hdf
    print hdf['sent']
    print hdf['/100/lists/tag']
    #print hdf['/100/names']
    print hdf['scheduler']


"""
ig = pd.read_csv("/home/david/fichiers/audit/igconseil/md5.csv")
print ig
cap = pd.read_csv("/home/david/fichiers/audit/cap/ExportPCP_22-july-2015_CompteTiers_[md5]_1400k.txt", names = ['md5'])
print cap
res = pd.merge(cap, ig)
print res
"""
"""
import DB_threaded_connection as DB
import pandas as pd
optin_dict = {4: 'youpick', 2: 'showroom'}
DB.initiate_threaded_connection_pool(DB.db_package)
with DB.getconnection() as connection:
    cursor = connection.cursor()
    select_query = "SELECT DISTINCT md5 FROM md5 m JOIN fichier_match f ON m.mail_id = f.mail_id WHERE f.fichier_id = '122';"
    df = pd.read_sql(select_query, connection)
    df.to_csv("/home/david/fichiers/audit/igconseil/md5.csv", index = False)
    print df
"""

"""
mbaz = mbaz_client(mbaz_dict, 'subscriber', 'showroom')
result = mbaz.service.GetSubscriber(1426577, {"int": [1, 32]})
print result
print result.fld
"""

"""import logging, sys
from suds.client import Client
handler = logging.StreamHandler(sys.stderr)
logger = logging.getLogger('suds.transport.http')
logger.setLevel(logging.DEBUG), handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

class OutgoingFilter(logging.Filter):
    def filter(self, record):
        return record.msg.startswith('sending:')

handler.addFilter(OutgoingFilter())

#client = Client(wsdl_url + "?WSDL", soapheaders = header_dict)
#client.set_options(soapheaders = header_dict)
print client
print "last_sent :"
print client.last_sent()
print "last_received :"
print client.last_received()

from suds.sax.element import Element
client = Client(wsdl_url + "?WSDL")
ssnns = ('ssn', 'http://namespaces/sessionid')
ssn = Element('Login', ns=ssnns).setText('user_MindbazWS_ShowroomStyliste')
client.set_options(soapheaders=ssn)

result = client.service.GetSubscriber(3)
print result
"""

# Attempt to implement pysimplesoap
"""
from pysimplesoap.client import SoapClient
from pysimplesoap.simplexml import SimpleXMLElement

namespace = 'http://www.mindbaz.com/webservices/'
auth_header_dict = {'Login' : Login, 'Password' : Password, 'IdSite' : IdSite}

def mindbaz_soap_client(wsdl_url, namespace, auth_dict):
    client = SoapClient(wsdl = wsdl_url + "?WSDL", trace=False)
    header = SimpleXMLElement('<Headers/>', namespace=namespace)
    auth_header = header.add_child(auth_string)
    auth_header['xmlns'] = namespace
    for key, value in auth_dict.iteritems():
        auth_header.marshall(key, value)
    client[auth_string] = auth_header
    return client

mbaz = mindbaz_soap_client(wsdl_url, namespace, auth_header_dict)
result = mbaz.GetSubscriberByEmail(email = "tinthoin@gmail.com")
print result
result = mbaz.GetSubscriber(1426577)
print result
"""

