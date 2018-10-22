import sys
import flask
from flask import Flask, jsonify
from flask_cors import CORS, cross_origin
from waitress import serve
from flask import request
import json
from getmeta import getmeta
from clusterdb import runTagging
from untagitems import untag

sys.path.append('.')

with open('./data/config.json') as f:
    cfg = json.load(f)

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route("/ping", methods=['GET'])
@cross_origin()
def ping():
    """Determine if the container is working and healthy. In this sample container, we declare
    it healthy if we can load the model successfully."""
    return flask.Response(response='\n', status=200, mimetype='application/json')


@app.route("/match", methods=['GET'])
@cross_origin()
def match():
    """ match faces for a specific entry.  pass in personId
    """

    personid = request.args.get('personid')

    if personid:
        return jsonify(runTagging(personid))

    return jsonify({'error': 'personid param not found in request'})


@app.route("/unmatch", methods=['GET'])
@cross_origin()
def unmatch():
    """ remove tagged items aka unmatch for a specific personId
    """
    personid = request.args.get('personid')

    if personid:
        return jsonify(untag(personid))

    return jsonify({'error': 'personid param not found in request'})


@app.route("/requestmeta", methods=['GET'])
@cross_origin()
def requestmeta():
    """
    :return: json data for the item with the personId passed
    """
    personid = request.args.get('personid')
    limit = request.args.get('limit')
    lastKeyTS = request.args.get('lastKeyTS')
    showTagged = request.args.get('showTagged')

    j = getmeta(personid, limit, showTagged, lastKeyTS)
    return jsonify(j)


print("Starting Face Recognition Server")
serve(app=app, port=cfg['port'])
