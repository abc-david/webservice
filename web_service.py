'''
Created on 24 mar. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-
#!flask/bin/python
from web_service_db_mapping import *
from web_service_background import Base, db_session
from flask import Flask, jsonify
from flask import abort



test_md5 = "c8fb9172158d23d9852ef9fd54958071"
test_md5 = "6f838fd82d44ba1d5c4b51b3fdf466f8"
#test_md5 = 25
print md5_2_mail(test_md5)[1]
print return_md5_request(test_md5)

app = Flask(__name__)

tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol', 
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web', 
        'done': False
    }
]

@app.route('/todo/api/v1.0/tasks/<int:task_id>', methods = ['GET'])
def get_task(task_id):
    task = filter(lambda t: t['id'] == task_id, tasks)
    if len(task) == 0:
        abort(404)
    return jsonify( { 'task': task[0] } )

@app.route('/mailAPI/<post_md5>', methods = ['GET'])
def retrieve_mail(post_md5):
    if post_md5:
        if "," in post_md5:
            md5_list = list(post_md5.split(','))
            api_answer = {}
            for md5_arg in md5_list:
                test = return_md5_request(md5_arg)[md5_arg]
                api_answer[md5_arg] = return_md5_request(md5_arg)[md5_arg]
        else: 
            api_answer = return_md5_request(post_md5)
        #print api_answer
        return jsonify(api_answer)
    return jsonify({'error' : "No 'md5' argument passed."})

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

#app.run(debug = True)
app.run('0.0.0.0')

#print retrieve_mail("123,125")
    
