import sys
import numpy as np
import json
import os
import time
import boto3
from boto3.dynamodb.conditions import Key, Attr
import io

sys.path.append('.')

with open('./data/config.json') as f:
    cfg = json.load(f)


def distance(emb1, emb2):
    return np.sqrt(np.sum(np.square(np.subtract(emb1, emb2))))


def runTagging(personId):
    """
    :param personId: the unique Id of the record to use as the source to cluster everything else against
    :return:
    """
    db = boto3.resource('dynamodb', region_name='us-east-1')
    thresh = cfg['cluster_face_distance']

    table = db.Table(cfg['person_recognition_table'])

    resp = table.query(ProjectionExpression='cameraId,ts,faces,personId',
                       IndexName='personId-index',
                       KeyConditionExpression=Key('personId').eq(personId))

    if resp['Count'] > 0:
        item = resp['Items'][0]
        faces = item['faces']
        cameraId = item['cameraId']
        timestamp = int(item['ts'])

        # convert embeddings back to numpy arrays
        for f in faces:
            f['embedding'] = np.fromstring(f['embedding'].value, dtype=np.float32)

        continue_scan = True

        cameraIds = []
        personIds = []
        timestamps = []
        last_evaluated_key = None

        while continue_scan:

            # iterate over entire database (TODO: Don may want to change how this works
            if last_evaluated_key:
                scan = table.scan(ProjectionExpression='cameraId, faces, ts, personId',
                                  ExclusiveStartKey=last_evaluated_key)
            else:
                scan = table.scan(ProjectionExpression='cameraId, faces, ts, personId')

            if scan['Count'] > 0:
                for i in scan['Items']:

                    if i['personId'] == item['personId']:
                        continue

                    if 'faces' in i:
                        for f2 in i['faces']:
                            for f3 in faces:
                                emb = np.fromstring(f2['embedding'].value, dtype=np.float32)
                                d = distance(emb, f3['embedding'])
                                if d <= thresh:
                                    cameraIds.append(i['cameraId'])
                                    personIds.append(i['personId'])
                                    timestamps.append(i['ts'])
                                    break


            if 'LastEvaluatedKey' in scan and cfg['max_matches'] < len(cameraIds):
                continue_scan = True
            else:
                continue_scan = False

            if continue_scan:
                last_evaluated_key = scan['LastEvaluatedKey']

        if len(cameraIds) > 0:
            update_resp = table.update_item(
                Key={
                    'cameraId': cameraId,
                    'ts': timestamp
                },
                UpdateExpression="set known= :t, matches= :c, tagged= :g",
                ExpressionAttributeValues={
                    ':t': True,
                    ':c': personIds,
                    ':g': False
                }
            )

            if update_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                return {
                    'error': "A response code of {} returned from updating table".format(update_resp['HTTPStatusCode'])}

            for p, t in zip(cameraIds, timestamps):
                update_resp = table.update_item(
                    Key={
                        'cameraId': p,
                        'ts': t
                    },
                    UpdateExpression="set known= :t, tagged= :c",
                    ExpressionAttributeValues={
                        ':t': False,
                        ':c': True
                    }
                )

                if update_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                    return {'error': "A response code of {} returned from updating tagged item in table".format(
                        update_resp['HTTPStatusCode'])}


    return {'status': True}


if __name__ == "__main__":
    if len(sys.argv) > 1:
        personId = sys.argv[1]
        runTagging(personId)
