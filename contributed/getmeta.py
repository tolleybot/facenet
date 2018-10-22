import sys
import json
import boto3
from boto3.dynamodb.conditions import Key

sys.path.append('.')

with open('./data/config.json') as f:
    cfg = json.load(f)


def getmeta(personId,limit=1000,showTagged=True,lastKeyTS=0):
    """
    Gets all meta data associated with a personId
    :param personId:
    :return: JSON
    """
    db = boto3.resource('dynamodb', region_name='us-east-1')
    table = db.Table(cfg['person_recognition_table'])

    if limit is None:
        limit = 500

    if lastKeyTS is None:
        lastKeyTS = 0

    if showTagged is None:
        showTagged = True

    limit = int(limit)
    lastKeyTS = int(lastKeyTS)
    showTagged = bool(int(showTagged))

    if personId:

        resp = table.query(ProjectionExpression='cameraId,ts,personId,videos,matches',
                           IndexName='personId-index',
                           KeyConditionExpression=Key('personId').eq(personId)
                           )

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

            return [{'cameraId' : item['cameraId'],
                    'personId': personId,
                    'ts': ts,
                     'matches':matches,
                    'videos': item['videos']
                    }]

        return {'info': 'personId {} not found'.format(personId)}

    else:

        items = []
        continue_scan = True
        last_evaluated_key = None

        while continue_scan:

            if last_evaluated_key:
                # grab x number of non-tagged items
                if showTagged:

                    resp = table.query(ProjectionExpression='cameraId,ts,personId,videos,known,tagged',
                                       KeyConditionExpression=Key('cameraId').eq('1') & Key('ts').lt(lastKeyTS),
                                       Limit=limit,
                                       ScanIndexForward=False,
                                       ExclusiveStartKey=last_evaluated_key
                                       )
                else:
                    resp = table.query(ProjectionExpression='cameraId,ts,personId,videos,known,tagged',
                                       KeyConditionExpression=Key('cameraId').eq('1') & Key('ts').lt(lastKeyTS),
                                       FilterExpression=Key('tagged').eq(False),
                                       Limit=limit,
                                       ScanIndexForward=False,
                                       ExclusiveStartKey=last_evaluated_key
                                       )
            else:
                if showTagged:

                    resp = table.query(ProjectionExpression='cameraId,ts,personId,videos,known,tagged',
                                       KeyConditionExpression=Key('cameraId').eq('1') & Key('ts').lt(lastKeyTS),
                                       Limit=limit,
                                       ScanIndexForward=False
                                       )
                else:
                    resp = table.query(ProjectionExpression='cameraId,ts,personId,videos,known,tagged',
                                       KeyConditionExpression=Key('cameraId').eq('1') & Key('ts').lt(lastKeyTS),
                                       FilterExpression=Key('tagged').eq(False),
                                       Limit=limit,
                                       ScanIndexForward=False
                                       )
            if 'LastEvaluatedKey' in resp:
                continue_scan = True
            else:
                continue_scan = False

            if continue_scan:
                last_evaluated_key = resp['LastEvaluatedKey']

            if 'Count' not in resp:
                return {'error': 'table query failed'}

            if resp['Count'] > 0:
                for i in resp['Items']:
                    j = {'cameraId' : i['cameraId'],
                         'personId': str(i['personId']),
                         'ts': int(i['ts']),
                         'videos': i['videos'],
                         'known' : bool(i['known']),
                         'tagged': bool(i['tagged'])
                         }
                    items.append(j)

    return items


