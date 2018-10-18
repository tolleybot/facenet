import sys
import numpy as np
import json
import time
import boto3
from boto3.dynamodb.conditions import Key, Attr
import io

sys.path.append('.')

with open('./config.json') as f:
    cfg = json.load(f)


def untag(personId):
    """
    :param personId: the id of the tagged item where you want to remove all of its match list
    :return:
    """
    db = boto3.resource('dynamodb', region_name='us-east-1')

    table = db.Table(cfg['person_recognition_table'])

    resp = table.query(KeyConditionExpression=Key('personId', ).eq(personId))

    if resp['Count'] > 0:
        item = resp['Items'][0]

        if 'matches' in item:

            matches = item['matches']

            for m in matches:

                resp_match = table.query(KeyConditionExpression=Key('personId', ).eq(m))

                if resp_match['ResponseMetadata']['HTTPStatusCode'] != 200:
                    print(
                        "A response code of {} returned from query for match item".format(resp_match['HTTPStatusCode']))
                    continue

                mitem = resp_match['Items'][0]

                update_resp = table.update_item(
                    Key={
                        'personId': mitem['personId'],
                        'ts': mitem['ts']
                    },
                    UpdateExpression="set known= :t, tagged= :c",
                    ExpressionAttributeValues={
                        ':t': False,
                        ':c': False
                    }
                )

                if update_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                    print("A response code of {} returned from updating tagged item in table".format(
                        update_resp['HTTPStatusCode']))

        # update known table to remove match list
        update_resp = table.update_item(
            Key={
                'personId': item['personId'],
                'ts': item['ts']
            },
            UpdateExpression="set known= :t, tagged= :c, matches= :m",
            ExpressionAttributeValues={
                ':t': False,
                ':c': False,
                ':m': []
            }
        )

        if update_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            print("A response code of {} returned from updating tagged item in table".format(
                update_resp['HTTPStatusCode']))

untag('a324412e-d2d9-11e8-9316-0242ac110004')