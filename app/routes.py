from flask import render_template
from app import app
#from dal.Dal import Dal 

@app.route('/')
def index():
    return ''' Hello world!'''

@app.route('/<cluster>', methods=['GET'])
def cluster_info(cluster):
    msg = "hello, %s"  % (cluster)
    return msg
    
