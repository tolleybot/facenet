import sys
import flask
from flask import Flask, jsonify
from waitress import serve
from flask import request
import json
from getmeta import getmeta

sys.path.append('.')

with open('./config.json') as f:
    cfg = json.load(f)

app = Flask(__name__)


@app.route("/ping", methods=['GET'])
def ping():
    """Determine if the container is working and healthy. In this sample container, we declare
    it healthy if we can load the model successfully."""
    return flask.Response(response='\n', status=200, mimetype='application/json')


@app.route("/match", methods=['GET'])
def match():
    """ match faces for a specific entry.  pass in personId
    """


@app.route("/unmatch", methods=['GET'])
def enroll():
    """ remove tagged items aka unmatch for a specific personId
    """


@app.route("/requestmeta", methods=['GET'])
def requestmeta():
    """
    :return: json data for the item with the personId passed
    """
    personid = request.args.get('personid')

    if personid:
        j = getmeta(personid)
        return jsonify(j)

    return jsonify({'error': 'personid param not fouund in request'})


print("Starting Face Recognition Server")
serve(app=app, port=8080)
