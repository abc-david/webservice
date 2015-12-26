'''
Created on 29 jan. 2015

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-
#!flask/bin/python

from flask import Flask, jsonify
from flask import abort
from test_request_wp_post import *

app = Flask(__name__)

@app.route('/testfeed', methods=['GET'])
def get_feed():
    feed = build_xml_feed('special')
    return feed
    
#@app.teardown_appcontext
#def shutdown_session(exception=None):
#    db_session.remove()

#app.run(debug = True)
app.run('0.0.0.0', threaded=True)

#if __name__ == '__main__':
#    app.run(debug=True)