#!/usr/bin/env python
import argparse
import collections
import os
import pprint

import boto3
import dotenv
import pendulum
from boto3.dynamodb.conditions import Attr, Key

dotenv.load_dotenv()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
assert DYNAMO_TABLE, 'Environment variable DYNAMO_TABLE must be defined'

POST_COMPLETED_STATUSES = ['COMPLETED', 'ARCHIVED', 'DELETING']


def parse_args():
    parser = argparse.ArgumentParser(description="Count users in each possible status in cognito")
    parser.add_argument(
        '-d',
        dest='date',
        required=True,
        type=lambda s: str(pendulum.parse(s).date()),
        help='Count users that signed up on this date. Ex: 2020-05-19',
    )
    args = parser.parse_args()
    return args.date


def generate_users(table, signed_up_date):
    "A generator that generates all users that signed up on the given date"
    # TODO: add an index to the user item, change this from a scan to a query
    kwargs = {
        'ProjectionExpression': 'userId',
        'FilterExpression': (
            Key('partitionKey').begins_with('user/')
            & Attr('signedUpAt').between(signed_up_date, signed_up_date + 'T24')
        ),
    }
    last_key = False
    while last_key is not None:
        if last_key:
            kwargs['ExclusiveStartKey'] = last_key
        resp = table.scan(**kwargs)
        for item in resp['Items']:
            yield item
        last_key = resp.get('LastEvaluatedKey')


def generate_posts(table, user_id):
    "A generator that generates all posts by that user"
    kwargs = {
        'ProjectionExpression': 'postId, postStatus, isVerified',
        'KeyConditionExpression': Key('gsiA2PartitionKey').eq(f'post/{user_id}'),
        'IndexName': 'GSI-A2',
    }
    last_key = False
    while last_key is not None:
        if last_key:
            kwargs['ExclusiveStartKey'] = last_key
        resp = table.query(**kwargs)
        for item in resp['Items']:
            yield item
        last_key = resp.get('LastEvaluatedKey')


def main():
    signed_up_date = parse_args()
    table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    stats = collections.defaultdict(int)
    for user in generate_users(table, signed_up_date):
        print('.', end='', flush=True)

        verified = False
        completed = False
        exists = False
        for post in generate_posts(table, user['userId']):
            if post.get('isVerified'):
                exists = completed = verified = True
                break
            elif post['postStatus'] in POST_COMPLETED_STATUSES:
                exists = completed = True
                continue
            else:
                exists = True

        if verified:
            stats['has_verified_post'] += 1
        if completed:
            stats['has_completed_post'] += 1
        if exists:
            stats['has_post'] += 1
        stats['total'] += 1

    print()
    pprint.pprint(dict(stats))


if __name__ == '__main__':
    main()
