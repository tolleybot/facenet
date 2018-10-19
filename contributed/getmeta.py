import sys
import json
import boto3
from boto3.dynamodb.conditions import Key

sys.path.append('.')

with open('./config.json') as f:
    cfg = json.load(f)


def getmeta(personId,limit=1000,lastKeyId=None,lastKeyTS=None):
    """
    Gets all meta data associated with a personId
    :param personId:
    :return: JSON
    """
    db = boto3.resource('dynamodb', region_name='us-east-1')
    table = db.Table(cfg['person_recognition_table'])

    if limit is None:
        limit = 500

    limit = int(limit)

    if personId:

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

    else:
        # grab x number of non-tagged items
        if lastKeyTS is not None and lastKeyId is not None:
            lastKeyTS = int(lastKeyTS)
            #lastKey = {'ts': boto3.dynamodb.types.Decimal(lastKeyTS)}
            resp = table.scan(ProjectionExpression='personId, videos, ts, known',
                              FilterExpression=Key('tagged').eq(False),
                              Limit=limit,
                              ExclusiveStartKey={'ts':lastKeyTS, 'personId': lastKeyId},
                              ScanIndexForward=True)
        else:
            resp = table.scan(ProjectionExpression='personId, videos, ts, known',
                              FilterExpression=Key('tagged').eq(False),
                              Limit=limit,
                              ScanIndexForward=True)

        items = []
        if resp['Count'] > 0:
            for i in resp['Items']:
                j = {'personId': str(i['personId']),
                     'ts': int(i['ts']),
                     'videos': i['videos'],
                     'known' : bool(i['known'])
                     }
                items.append(j)

        if 'LastEvaluatedKey' in resp:
            j = {'lastKey': {'personId': str(resp['LastEvaluatedKey']['personId']),
                             'ts': int(resp['LastEvaluatedKey']['ts'])},
                 'items': items}
        else:
            j = {'items': items}

        return j


