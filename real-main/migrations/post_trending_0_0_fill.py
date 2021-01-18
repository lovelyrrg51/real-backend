import decimal
import logging
import os
import random

import boto3
import pendulum

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "Add a post trending for every completed post that doesn't already have one"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        # This doesn't scale.
        # We only have ~3k completed posts in production DB right now so we can
        # get away with loading all their id's into memory like this.
        completed_post_ids = set(self.generate_completed_post_ids())
        trending_post_ids = set(self.generate_trending_post_ids())
        post_ids_to_add = completed_post_ids - trending_post_ids
        now = pendulum.now('utc')
        for post_id in post_ids_to_add:
            # score in [0.5, 1)
            score = decimal.Decimal(random.random() / 2 + 0.5).normalize()
            self.add_trending(post_id, score, now)

    def generate_completed_post_ids(self):
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND postStatus = :ps',
            'ExpressionAttributeValues': {':pk_prefix': 'post/', ':ps': 'COMPLETED'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item['postId']
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def generate_trending_post_ids(self):
        query_kwargs = {
            'KeyConditionExpression': 'gsiK3PartitionKey = :gsik3pk',
            'ExpressionAttributeValues': {':gsik3pk': 'post/trending'},
            'IndexName': 'GSI-K3',
        }
        while True:
            paginated = self.dynamo_table.query(**query_kwargs)
            for item in paginated['Items']:
                yield item['partitionKey'].split('/')[1]
            if 'LastEvaluatedKey' not in paginated:
                break
            query_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def add_trending(self, post_id, score, now):
        assert isinstance(score, decimal.Decimal), 'Boto uses decimals for numbers'
        assert score > 0, 'Score must be greater than 0'

        now_str = now.to_iso8601_string()
        query_kwargs = {
            'Item': {
                'partitionKey': f'post/{post_id}',
                'sortKey': 'trending',
                'schemaVersion': 0,
                'gsiK3PartitionKey': 'post/trending',
                'gsiK3SortKey': score,
                'lastDeflatedAt': now_str,
                'createdAt': now_str,
            },
            'ConditionExpression': 'attribute_not_exists(partitionKey)',
        }
        try:
            logger.warning(f'Post `{post_id}`: adding trending')
            self.dynamo_table.put_item(**query_kwargs)
        except self.dynamo_client.exceptions.ConditionalCheckFailedException:
            logger.warning(f'Post `{post_id}`: adding trending - FAILED, skipping')


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_client = boto3.client('dynamodb')
    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
