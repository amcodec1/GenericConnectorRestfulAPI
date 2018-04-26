from flask import Flask, jsonify, request
from flask.ext.pymongo import PyMongo
from flask import render_template
from flask import jsonify
from flask import redirect
from flask import Blueprint
from flask.ext.paginate import Pagination
from flask.ext.mongoengine import MongoEngine
from pymongo import read_preferences
from flask.ext.mongoengine.wtf import model_form
from math import ceil
import json
import libvirt
import os
import numpy
import sys
from sh import ssh

app = Flask(__name__)

######## Global Variables #############


app.config['MONGO_DBNAME'] = 'databasename'
app.config['MONGO_URI'] = 'mongodb://username:password@hostname:port/databasename'

mongo_host=localhost
mongo_port=27017
mongo_db=client.local
mongo_collection=db.transactions

mongo = PyMongo(app)

def parse_propeties_file(file):
    
    separator = "="
    keys = {}
    with open(file) as f:
        for line in f:
            if separator in line:                                
                name, value = line.split(separator, 1)
                keys[name.strip()] = value.strip()
                
    return keys
	
@app.route('/kafka/<kafka_topic>/<kafka_group_id>', methods=['GET'])	
def get_all_kafka_events(kafka_topic,kafka_group_id):   
    print("Inside Kafka connector")
    consumer = KafkaConsumer(kafka_topic, group_id = kafka_group_id,\
                                 bootstrap_servers=eval(keys['kafka_server']))
    kafka_result = []									
    for msg in consumer:	
        data= msg.value
        kafka_result.append(data)		
        consumer.close()
    return kafka_result
	
@app.route('/hdfs', methods=['GET'])		
def get_all_hdfs_data():    
    hdfs = HDFileSystem(keys["hdfs_host"], keys["hdfs_port"], user)    
	hdfs_result=[]
    with hdfs.open(filename, 'rb') as f:        
        contents = f.read()
        hdfs_result.append({'Contents' : contents})
	
	return jsonify({'hdfs_data' : hdfs_result})
        


@app.route('/mongo/', methods=['GET'])
def get_all_frameworks():
    framework = mongo.db.framework 

    output = []

    for q in framework.find():
        output.append({'name' : q['name'], 'language' : q['language']})

    return jsonify({'result' : output})


	
	
	
######## Cassandra Import Statements ###########
from cassandra.cluster import Cluster

from cassandra.query import SimpleStatement









####################### Code related to KeySpace Operations #######################


@app.route('/keyspaces', methods=['GET'])
def getKeyspaces(page):
    cluster = Cluster()
    keypspace = 'northwind'
    students = []
    connection=cluster.connect(keyspace)
    students = connection.execute("SELECT * FROM student_info")
    for student in students:
        students.append[{'student_roll_no' : student.student_roll_no,'student_city' :student.student_city, 'student_fees' : student.student_fees,'student_name' :student.student_name,'student_phone' :student.student_phone}]
        
    return jsonify({'students' : students})
        
        
	

	

if __name__ == '__main__':
    app.run(debug=True)