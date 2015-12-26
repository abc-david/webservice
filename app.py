''' Created on 4 avril 2015 '''
#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'david'

from flask import request, Flask
app = Flask(__name__)

""" Error Handling & Logging """
ADMINS = ['tinthoin@gmail.com']
if not app.debug:
    import logging
    from logging.handlers import SMTPHandler
    mail_handler = SMTPHandler('127.0.0.1',
                               'server-error@example.com',
                               ADMINS, 'Webservice Error : Flask Failed')
    mail_handler.setLevel(logging.ERROR)
    app.logger.addHandler(mail_handler)

if not app.debug:
    import logging
    from logging import Formatter
    from logging import FileHandler
    file_handler = FileHandler("log_flask_errors.txt")
    file_handler.setFormatter(Formatter('%(asctime)s %(levelname)s: %(message)s ''[in %(pathname)s:%(lineno)d]'))
    file_handler.setLevel(logging.WARNING)
    app.logger.addHandler(file_handler)

"""@app.errorhandler(500)
def internal_error(exception):
    app.logger.exception(exception)
    return render_template('500.html'), 500
"""

""" PCP stuff """
#from pcp_webservice_debug import *
from pcp_webservice import *

@app.route('/pcp')
def index_pcp():
    return 'Flask is running!'

@app.route('/pcp/feed.xml', methods=['GET'])
def get_feed():
    feed = build_xml_feed('special')
    return feed

""" Criteo stuff (youpick, showroom, wemag, newsdemode, daydreamblog, reponsesauquotidien, bonsplansmag) """
from criteo_webservice import *

@app.route('/criteo', methods=['GET', 'POST'])
def index_criteo():
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

@app.route('/criteo/data/wem/', methods=['GET', 'POST'])
def data_wem():
    md5_list = list(request.form.itervalues())
    return criteo_webservice(md5_list, 'wem', db_package)

@app.route('/criteo/data/new/', methods=['GET', 'POST'])
def data_new():
    md5_list = list(request.form.itervalues())
    return criteo_webservice(md5_list, 'new', db_package)

@app.route('/criteo/data/day/', methods=['GET', 'POST'])
def data_day():
    md5_list = list(request.form.itervalues())
    return criteo_webservice(md5_list, 'day', db_package)

@app.route('/criteo/data/rep/', methods=['GET', 'POST'])
def data_rep():
    md5_list = list(request.form.itervalues())
    return criteo_webservice(md5_list, 'rep', db_package)

@app.route('/criteo/data/bon/', methods=['GET', 'POST'])
def data_bon():
    md5_list = list(request.form.itervalues())
    return criteo_webservice(md5_list, 'bon', db_package)

@app.route('/criteo/xml/you/', methods=['GET', 'POST'])
def xml_you():
    return get_xml('you', db_package)

@app.route('/criteo/xml/sho/', methods=['GET', 'POST'])
def xml_sho():
    return get_xml('sho', db_package)

@app.route('/criteo/xml/wem/', methods=['GET', 'POST'])
def xml_wem():
    return get_xml('wem', db_package)

@app.route('/criteo/xml/new/', methods=['GET', 'POST'])
def xml_new():
    return get_xml('new', db_package)

@app.route('/criteo/xml/day/', methods=['GET', 'POST'])
def xml_day():
    return get_xml('day', db_package)

@app.route('/criteo/xml/rep/', methods=['GET', 'POST'])
def xml_rep():
    return get_xml('rep', db_package)

@app.route('/criteo/xml/bon/', methods=['GET', 'POST'])
def xml_bon():
    return get_xml('bon', db_package)

""" Adlead stuff (youpick, showroom, wemag, newsdemode, daydreamblog, reponsesauquotidien, bonsplansmag) """
from adlead_webservice import *

@app.route('/adlead', methods=['GET', 'POST'])
def index_adlead():
    return 'Flask is running'

@app.route('/adlead/data/you/', methods=['GET', 'POST'])
def adlead_data_you():
    md5_list = list(request.form.itervalues())
    #print md5_list
    json_res = adlead_webservice(md5_list, 'you', db_package)
    #print "OK json"
    return json_res

@app.route('/adlead/data/sho/', methods=['GET', 'POST'])
def adlead_data_sho():
    md5_list = list(request.form.itervalues())
    return adlead_webservice(md5_list, 'sho', db_package)

@app.route('/adlead/data/wem/', methods=['GET', 'POST'])
def adlead_data_wem():
    md5_list = list(request.form.itervalues())
    return adlead_webservice(md5_list, 'wem', db_package)

@app.route('/adlead/data/new/', methods=['GET', 'POST'])
def adlead_data_new():
    md5_list = list(request.form.itervalues())
    return adlead_webservice(md5_list, 'new', db_package)

@app.route('/adlead/data/day/', methods=['GET', 'POST'])
def adlead_data_day():
    md5_list = list(request.form.itervalues())
    return adlead_webservice(md5_list, 'day', db_package)

@app.route('/adlead/data/rep/', methods=['GET', 'POST'])
def adlead_data_rep():
    md5_list = list(request.form.itervalues())
    return adlead_webservice(md5_list, 'rep', db_package)

@app.route('/adlead/data/bon/', methods=['GET', 'POST'])
def adlead_data_bon():
    md5_list = list(request.form.itervalues())
    return adlead_webservice(md5_list, 'bon', db_package)

@app.route('/adlead/xml/you/', methods=['GET', 'POST'])
def adlead_xml_you():
    return get_xml('you', db_package)

@app.route('/adlead/xml/sho/', methods=['GET', 'POST'])
def adlead_xml_sho():
    return get_xml('sho', db_package)

@app.route('/adlead/xml/wem/', methods=['GET', 'POST'])
def adlead_xml_wem():
    return get_xml('wem', db_package)

@app.route('/adlead/xml/new/', methods=['GET', 'POST'])
def adlead_xml_new():
    return get_xml('new', db_package)

@app.route('/adlead/xml/day/', methods=['GET', 'POST'])
def adlead_xml_day():
    return get_xml('day', db_package)

@app.route('/adlead/xml/rep/', methods=['GET', 'POST'])
def adlead_xml_rep():
    return get_xml('rep', db_package)

""" Launch Flask webservice """

if __name__ == '__main__':
    app.run('0.0.0.0', threaded=True)
    #app.run(debug=True)
