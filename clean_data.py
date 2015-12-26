''' Created on November 4 2015 '''
#!/usr/bin/env python
# coding: utf-8
from __future__ import unicode_literals

import re
import numpy as np
import dateutil.parser as dparser
from dateutil.relativedelta import relativedelta

def convert_dataframe_scalar(np_value):
    if np_value != "":
        try:
            py_value = np_value.encode('utf-8').strip()
        except:
            try:
                py_value = np.asscalar(np_value)
            except:
                return [False, np_value, "Failed"]
        return [True, py_value]
    else:
        return [False, np_value, "Empty"]

def check_for_bad_text(string):
    return string
    """unicode_string = unicode(string)
    num_ascii_list =[153,154,158,162,163,164,165,166,167,169,170,171,174,188,189,190,191,192,193,194,195,196,197]
    ascii_list = [unichr(i) for i in num_ascii_list]
    for ascii_char in ascii_list:
        if ascii_char in unicode_string:
            #print unicode_string
            return fix_bad_unicode(unicode_string)
    return string"""

def clean_separator(value, separator_dict, universal_separator, city_mode = False):
    city_word = ['le', 'la', 'de', 'du', 'des', 'les', 'a', 'aux', 'lez', 'en', 'dans', 'sous', 'sur']
    particule_word = ['de', 'du', 'des']
    try:
        for separator, separator_list in separator_dict.iteritems():
            for item in separator_list:
                if item in value:
                    word_list = list(value.split(item))
                    new_word_list = []
                    for word in word_list:
                        word = word.strip()
                        if not word.isdigit():
                            if city_mode and word in city_word:
                                word = word.lower()
                            elif not city_mode and word in particule_word:
                                word = [word.lower(), 'particule']
                            else:
                                try:
                                    if "d'" in word:
                                        pos = word.find("d'")
                                        word = word[:pos+1].lower() + word[pos+2].upper() + word[pos+3:].lower()
                                    else:
                                        word = word[0].upper() + word[1:].lower()
                                except:
                                    pass
                            new_word_list.append(word)
                    if len(new_word_list) > 1:
                        if not city_mode:
                            for word in new_word_list:
                                if isinstance(word, list):
                                    if word[1] == 'particule':
                                        particule_list = []
                                        for word in new_word_list:
                                            if isinstance(word, list):
                                                particule_list.append(word[0].lower())
                                            else:
                                                particule_list.append(word[0].upper() + word[1:].lower())
                                        try:
                                            new_value  = str(" ".join(particule_list))
                                        except:
                                            new_value = value
                                        return new_value
                    try:
                        if universal_separator:
                            new_value = str(universal_separator.join(new_word_list))
                        else:
                            new_value = str(separator.join(new_word_list))
                    except:
                        new_value = value
                    return new_value
        if not value.isdigit():
            new_value = value[0].upper() + value[1:].lower()
            return new_value
        else:
            return value
    except:
        return value

def clean_name(value, separator_check = True, city_mode = False):
    separator_dict = {'-' : ['----', '---', '--', '-'], \
                      '/' : ['////', '///', '//', '/'], \
                      '_' : ['____', '___', '__', '_'], \
                      ":" : ["::::",":::", "::", ":"], \
                      "#" : ["####", "###", "##", "#"], \
                      '~' : ['~~~~', '~~~', '~~', '~'], \
                      "'" : ["''''","'''", "''"], \
                      ',' : [',,,,', ',,,', ',,'], \
                      ';' : [';;;;', ';;;', ';;']}
    if isinstance(value, basestring):
        value = ' '.join(value.split())
        value = value.replace('\\', '')
        value = value.replace('?', 'e')
        if separator_check and separator_dict:
            value = clean_separator(value, separator_dict, '-', city_mode = city_mode)
        else:
            if not value.isdigit():
                try:
                    value = value[0].upper() + value[1:].lower()
                except:
                    pass
        if len(value) > 40:
            value = value[:40]
        return value
    else:
        return ""

def clean_ville(value):
    return clean_name(value, city_mode = True)

def clean_cp(value):
    if value.isdigit():
        len_value = len(str(value))
        if len_value == 5:
            return str(value)
        elif len_value == 4:
            value = "0" + str(value)
            return str(value)
        elif len_value == 3:
            value = "0" + str(value) + "0"
            return str(value)
        else:
            return ""
    else:
        return ""

def clean_num(value):
    replace_list = [".", "+33", "0033"]
    value = value.replace(" ", "")
    for item in replace_list:
        value = value.replace(item, "")
    if value.isdigit():
        len_value = len(str(value))
        if len_value == 10:
            str_value = str(value)
            return str_value[1:]
        elif len_value == 9:
            return str(value)
    return ""

def clean_port(value):
    port = clean_num(value)
    if port:
        if port[0] in ["6", "7"]:
            return port
    return ""

def clean_tel(value):
    port = clean_num(value)
    if port:
        if port[0] not in ["6", "7"]:
            return port
    return ""

def clean_civilite(value):
    if isinstance(value, basestring):
        if not value.isdigit():
            convert_civilite = {'M' : 1, 'Mr' : 1, 'M.' : 1, "Mr." : 1, "Monsieur" : 1, \
                                "Mademoiselle" : 2, "Mlle" : 2, \
                                "Madame" : 3, "Mme" : 3}
            if value in convert_civilite:
                return convert_civilite[value]
            else:
                for key, return_value in convert_civilite.iteritems():
                    if value.lower() == str(key).lower():
                        return return_value
            return 1
        else:
            if value in ['1', '2', '3']:
                return value
            else:
                return 1
    elif isinstance(value, (int, long)):
        if value in [1, 2, 3]:
            return value
        else:
            return 1
    else:
        return ""

def clean_birth(value, dayfirst = True):
    if isinstance(value, basestring):
        try:
            parsed_date = dparser.parse(value, fuzzy = True, dayfirst = dayfirst)
        except:
            return ""
        if parsed_date.year < 1900:
            parsed_date = parsed_date + relativedelta(years = 100)
        elif parsed_date.year > 2000:
            parsed_date = parsed_date - relativedelta(years = 100)
        if (parsed_date.year < 1900) or (parsed_date.year > 2000):
            return ""
        if parsed_date.year == 1900:
            if (parsed_date.month == 1) and (parsed_date.day == 1):
                return ""
        try:
            #formatted_date = parsed_date.strftime('%d/%m/%y')
            formatted_date = parsed_date.isoformat()
        except:
            try:
                print "date problem :" + str(parsed_date)
            except:
                pass
            return ""
        #print "clean_birth : " + value + " --> " + str(parsed_date) + " --> " + str(formatted_date)
        return formatted_date
    else:
        return value.isoformat()

def clean_ip(value):
    if isinstance(value, basestring):
        if value.count('.') == 3:
            return value
    return ""

def clean_provenance(value):
    if isinstance(value, basestring):
        return value
    else:
        return ""

def clean_date(value, dayfirst = True):
    if isinstance(value, basestring):
        parsed_date = dparser.parse(value, fuzzy = True, dayfirst = dayfirst)
        try:
            parsed_date = dparser.parse(value, fuzzy = True, dayfirst = dayfirst)
        except:
            return ""
        if parsed_date.year < 2000:
            parsed_date = parsed_date + relativedelta(years = 100)
        try:
            #formatted_date = parsed_date.strftime('%d/%m/%y')
            formatted_date = parsed_date.isoformat()
        except:
            try:
                print "date problem :" + str(parsed_date)
            except:
                pass
            return ""
        #print "clean_birth : " + value + " --> " + str(parsed_date) + " --> " + str(formatted_date)
        return formatted_date
    else:
        return ""

def clean_int_score(value):
    if value:
        if value != "NaN":
            try:
                new_dict = dict((int(k), int(v)) for k, v in \
                            (part.split(': ') for part in \
                             re.sub('[{}]', '', value).split(', ')))
                return new_dict
            except:
                return ""
    return ""

def clean_data(dataframe, field):
    cleaning_scripts = {'prenom' : clean_name, 'nom' : clean_name, 'cp' : clean_cp, \
                        'ville' : clean_ville, 'civilite' : clean_civilite, 'birth' : clean_birth, \
                        'ip' : clean_ip, 'provenance' : clean_provenance, 'date' : clean_date, \
                        'port' : clean_port, 'tel' : clean_tel, 'tel1' : clean_tel, 'tel2' : clean_tel, 'fax' : clean_tel}
                        #'score' : clean_int_score} #depreciated because dataframe cannot store dicts
    string_fields = ['cp', 'port', 'tel', 'tel1', 'tel2', 'fax']
    milestones = {1 : 1000, 2 : 2000, 3 : 5000, 4 : 10000, 5 : 20000, 6 : 50000, \
                  7 : 100000, 8 : 200000, 9 : 500000, 10 : 1000000, 11 : 2000000, 12: 5000000}
    cpt_milestones = 1
    if field in list(cleaning_scripts.keys()):
        if field in string_fields:
            try:
                dataframe[field] = dataframe[field].astype('string', copy = True)
            except:
                for cpt in range(len(dataframe.index)):
                    try:
                        dataframe.at[dataframe.index[cpt], field] = str(dataframe.at[dataframe.index[cpt], field])
                    except:
                        dataframe.at[dataframe.index[cpt], field] = ""
            #show_df(dataframe[field])
            try:
                dataframe[field] = dataframe[field].str.strip()
            except:
                pass
        cleaning_function = cleaning_scripts[field]
        index = dataframe.index
        fix_list = []
        for cpt in range(len(index)):
            """
            if cpt == milestones[cpt_milestones]:
                message = "OK. First %s data for field '%s' looked up." % (str(milestones[cpt_milestones]), str(field))
                print_to_log(log_file, 4, message)
                cpt_milestones += 1
                #print_to_log(log_file, 4, str(fix_list))
            """
            np_value = dataframe.at[index[cpt], field]
            py_convert = convert_dataframe_scalar(np_value)
            if py_convert[0]:
                raw_value = py_convert[1]
                unicode_clean_value = check_for_bad_text(raw_value)
                if raw_value != unicode_clean_value:
                    this_fix = [raw_value, unicode_clean_value]
                    fix_list.append(this_fix)
                clean_value = cleaning_function(unicode_clean_value)
                for csv_separator in [';', ',']:
                    try:
                        clean_value = clean_value.replace(csv_separator, '')
                    except:
                        clean_value = str(clean_value)
                        clean_value = clean_value.replace(csv_separator, '')
            else:
                clean_value = ""
            dataframe.at[index[cpt], field] = clean_value
        """
        if len(fix_list) > 0:
            message = "OK : Data fixed for bad unicode in %s occasions." % str(len(fix_list))
            print_to_log(log_file, 3, message)
            for item in fix_list:
                print " --> ".join(item)
        """
        return dataframe
    else:
        return dataframe