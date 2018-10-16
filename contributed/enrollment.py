import os
import getopt
import sys
import flask
from PIL import Image
from io import BytesIO
import base64
import numpy as np
import json
import time
import face
import cv2
import uuid
import boto3
import botocore
import io

sys.path.append('.')

with open('./config.json') as f:
    cfg = json.load(f)


detector = face.Detection(face_crop_size=cfg['face_crop_size'], face_crop_margin=['face_crop_margin'])
encoder = face.Encoder(face_model=cfg['facenet_modelpath'])


def runSQSPoller():
    """ Polls our SQS to see if there are any new messages
    to act on"""

    sqs = boto3.resource('sqs', region_name='us-east-1')
    s3 = boto3.resource('s3', region_name='us-east-1')
    queue = sqs.get_queue_by_name(QueueName=cfg['thumbnail_sqs_name'])

    while True:

        for message in queue.receive_messages():
            print(message.body)
            data = json.loads(message.body)

            if len(data['Records']) > 0:
                record = data['Records'][0]
                s3data = record['s3']
                bucket_name = s3data['bucket']['name']
                image_name = s3data['object']['key']

                try:
                    obj = s3.Object(bucket_name,image_name)
                    buffer = io.BytesIO(obj.get()["Body"].read())
                    image = Image.open(buffer)
                    print(image.width)
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("The object does not exist.")

                    #TODO remove this message here
                    continue




        time.sleep(0.1)




runSQSPoller()