import sys
import json
import boto3
from boto3.dynamodb.conditions import Key

sys.path.append('.')

with open('./config.json') as f:
    cfg = json.load(f)


def getmeta(personId):
    """
    Gets all meta data associated with a personId
    :param personId:
    :return: JSON
    """
    db = boto3.resource('dynamodb', region_name='us-east-1')
    table = db.Table(cfg['person_recognition_table'])

    resp = table.query(KeyConditionExpression=Key('personId', ).eq(personId))

    if resp['Count'] > 0:
        item = resp['Items'][0]

        faces = []
        if 'faces' in item:
            for f in item['faces']:
                tlx = int(f['bbox'][0])
                tly = int(f['bbox'][1])
                blx = int(f['bbox'][2])
                bly = int(f['bbox'][3])
                faces.append([tlx, tly, blx, bly])

        matches = []
        if 'matches' in item:
            for m in item['matches']:
                matches.append(str(m))

        known = False
        if 'known' in item:
            known = bool(item['known'])

        tagged = False
        if 'tagged' in item:
            tagged = bool(item['tagged'])

        ts = int(item['ts'])

        videos = []
        if 'videos' in item:
            for v in item['videos']:
                videos.append(str(v['image']))

        return {'personId': personId,
                'ts': ts,
                'faces': faces,
                'known': known,
                'tagged': tagged,
                'matches': matches,
                'videos': videos
                }

    return {'info': 'personId {} not found'.format(personId)}

#j = getmeta('a324412e-d2d9-11e8-9316-0242ac110004')
#print(j)

