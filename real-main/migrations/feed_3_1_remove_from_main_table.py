import json
import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Copy feed items from main table to the feed table"

    def __init__(self, dynamo_client, dynamo_main_table):
        self.dynamo_client = dynamo_client
        self.dynamo_main_table = dynamo_main_table

    def run(self):
        key_generator = self.generate_old_feed_keys()
        self.delete_items(key_generator)

    def generate_old_feed_keys(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
            'ProjectionExpression': 'partitionKey, sortKey',
            'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sk_prefix': 'feed/'},
        }
        while True:
            paginated = self.dynamo_main_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def delete_items(self, key_generator):
        with self.dynamo_main_table.batch_writer() as batch:
            for key in key_generator:
                logger.warning(f'Deleting feed item `{key}`')
                batch.delete_item(Key=key)


def lambda_handler(event, context):
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo main table name'

    dynamo_client = boto3.client('dynamodb')
    dynamo_main_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    migration = Migration(dynamo_client, dynamo_main_table)
    migration.run()

    return {'statusCode': 200, 'body': json.dumps('Migration completed successfully')}


if __name__ == '__main__':
    lambda_handler(None, None)
