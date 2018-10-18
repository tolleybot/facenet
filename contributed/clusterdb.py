import sys
import numpy as np
import json
import os
import time
import boto3
from boto3.dynamodb.conditions import Key, Attr
import io

sys.path.append('.')

with open('./config.json') as f:
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

    resp = table.query(KeyConditionExpression=Key('personId', ).eq(personId))

    if resp['Count'] > 0:
        item = resp['Items'][0]
        faces = item['faces']
        timestamp = int(item['ts'])

        # convert embeddings back to numpy arrays
        for f in faces:
            f['embedding'] = np.fromstring(f['embedding'].value, dtype=np.float32)

        # iterate over entire database for a specific time period

        scan = table.scan(ProjectionExpression='personId, faces, ts')

        personIds = []
        timestamps = []
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
                                personIds.append(i['personId'])
                                timestamps.append(i['ts'])
                                break

        if len(personIds) > 0:
            update_resp = table.update_item(
                Key={
                    'personId': personId,
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
                print("A response code of {} returned from updating table".format(update_resp['HTTPStatusCode']))

            for p, t in zip(personIds, timestamps):
                update_resp = table.update_item(
                    Key={
                        'personId': p,
                        'ts': t
                    },
                    UpdateExpression="set known= :t, tagged= :c",
                    ExpressionAttributeValues={
                        ':t': False,
                        ':c': True
                    }
                )

                if update_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                    print("A response code of {} returned from updating tagged item in table".format(
                        update_resp['HTTPStatusCode']))
            print('finished')


if __name__ == "__main__":
    if len(sys.argv) > 2:
        personId = sys.argv[2]
        runTagging(personId)
