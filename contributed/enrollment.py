import sys
from PIL import Image
import numpy as np
import json
import time
import face
import uuid
import boto3
import botocore
import io

sys.path.append('.')

with open('./config.json') as f:
    cfg = json.load(f)

detector = face.Detection(face_crop_size=cfg['face_crop_size'], face_crop_margin=cfg['face_crop_margin'])
encoder = face.Encoder(facenet_model=cfg['facenet_modelpath'])


def get_object_url(s3_bucket_name, key_name):
    # Get the service client.
    s3 = boto3.client('s3')

    # Generate the URL to get 'key-name' from 'bucket-name'
    url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': s3_bucket_name,
            'Key': key_name
        },
        ExpiresIn=604800
    )

    return url


def runSQSPoller():
    """ Polls our SQS to see if there are any new messages
    to act on"""

    sqs = boto3.resource('sqs', region_name='us-east-1')
    s3 = boto3.resource('s3', region_name='us-east-1')
    db = boto3.resource('dynamodb', region_name='us-east-1')
    queue = sqs.get_queue_by_name(QueueName=cfg['thumbnail_sqs_name'])

    while True:

        for message in queue.receive_messages():

            data = json.loads(message.body)

            if 'Records' not in data:
                message.delete()
                continue

            if len(data['Records']) > 0:
                record = data['Records'][0]
                s3data = record['s3']
                bucket_name = s3data['bucket']['name']
                image_name = s3data['object']['key']

                try:
                    obj = s3.Object(bucket_name, image_name)
                    obj_url = get_object_url(bucket_name, image_name)
                    buffer = io.BytesIO(obj.get()["Body"].read())
                    image = np.asarray(Image.open(buffer))

                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("The object does not exist.")

                    message.delete()
                    continue

                faces = detector.find_faces(image)
                minarea = cfg['face_min_area']
                updated_faces = []
                if len(faces) > 0:
                    # get largest face
                    for fc in faces:
                        tx = fc.bounding_box[0]
                        ty = fc.bounding_box[1]
                        bx = fc.bounding_box[2]
                        by = fc.bounding_box[3]
                        area = (bx - tx) * (by - ty)
                        if area > minarea:
                            emb = encoder.generate_embedding(fc)
                            updated_faces += [{'bbox': fc.bounding_box.tolist(), 'embedding': emb.tostring()}]

                    if len(updated_faces) == 0:
                        print("Only small faces found < {}.  Faces will not be added".format(cfg['face_min_area']))
                        message.delete()
                        continue

                    print("Adding Face")
                    rec_table = db.Table(cfg['person_recognition_table'])

                    rec_table.put_item(
                        Item={
                            'known': False, # Indicates this entry was clustered with a tagged entry
                            'tagged': False, # Indicates this entry was picked by the user
                            'personId': str(uuid.uuid1()),
                            'ts': int(time.time()),
                            'videos': [{'image': obj_url, 'imageId': str(uuid.uuid1())}],
                            'faces': updated_faces
                        }
                    )

                    message.delete()

        time.sleep(cfg['input_queue_polling_delay'])
