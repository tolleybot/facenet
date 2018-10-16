import os
import getopt
import sys
import flask
from flask import Flask
from waitress import serve
from PIL import Image
from io import BytesIO
import base64
import numpy as np
import json
import face
import cv2
import uuid
sys.path.append('.')
from enrollment import runSQSPoller

runSQSPoller()

with open('./config.json') as f:
    cfg = json.load(f)

app = Flask(__name__)

recognition = face.Recognition(face_crop_size=cfg['face_crop_size'], face_crop_margin=['face_crop_margin'])

@app.route("/ping", methods=['GET'])
def ping():
    """Determine if the container is working and healthy. In this sample container, we declare
    it healthy if we can load the model successfully."""
    if recognition is None:
        status = 404
    else:
        status = 200

    return flask.Response(response='\n', status=status, mimetype='application/json')


@app.route("/enroll", methods=['POST'])
def enroll():
    """enroll a new person or add this person to a known person
    """
    if recognition is None:
        return flask.Response(response='Detector is not initialized', status=404, mimetype='text/plain')

    if 'image/' in flask.request.content_type:
        data = flask.request.data.decode('utf-8')
        img = Image.open(BytesIO(base64.b64decode(data)))

        # TODO: Don do you want to add multiple images?
        # TODO: Do you want to try to identify first and then add identity?
        faces = recognition.add_identity(image=img,person_name=str(uuid.uuid1()))

        if faces is None or len(faces) == 0:
            return flask.Response(response='No faces were found or More than one face was in the image', status=501, mimetype='text/plain')


        # TODO: Don add to Face to dynamodb







@app.route("/recognize", methods=['POST'])
def recognize():
    """
     runs detection on image or video
    """
    if recognition is None:
        return flask.Response(response='Detector is not initialized', status=404, mimetype='text/plain')


print("Starting Face Recognition Server")
serve(app=app, port=8080)