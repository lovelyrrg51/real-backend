import json
import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
DYNAMO_FEED_TABLE = os.environ.get('DYNAMO_FEED_TABLE')

logger = logging.getLogger()


class Migration:
    "Copy feed items from main table to the feed table"

    def __init__(self, dynamo_client, dynamo_main_table, dynamo_feed_table):
        self.dynamo_client = dynamo_client
        self.dynamo_main_table = dynamo_main_table
        self.dynamo_feed_table = dynamo_feed_table

    def run(self):
        old_feed_item_generator = self.generate_old_feed_items_to_migrate()
        new_feed_item_generator = (self.transform_item(i) for i in old_feed_item_generator)
        self.write_new_feed_items(new_feed_item_generator)

    def generate_old_feed_items_to_migrate(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
            'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sk_prefix': 'feed/'},
        }
        while True:
            paginated = self.dynamo_main_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def transform_item(self, old_item):
        post_id = old_item['partitionKey'].split('/')[1]
        posted_at = old_item['gsiA1SortKey']
        posted_by_user_id = old_item['gsiA2SortKey']
        feed_user_id = old_item['sortKey'].split('/')[1]
        return {
            'postId': post_id,
            'postedByUserId': posted_by_user_id,
            'postedAt': posted_at,
            'feedUserId': feed_user_id,
        }

    def write_new_feed_items(self, new_item_generator):
        with self.dynamo_feed_table.batch_writer() as batch:
            for item in new_item_generator:
                post_id, feed_user_id = item["postId"], item["feedUserId"]
                logger.warning(f'Migrating feed item (`{post_id}`, `{feed_user_id}`)')
                batch.put_item(Item=item)


def lambda_handler(event, context):
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo main table name'
    assert DYNAMO_FEED_TABLE, 'Must set env variable DYNAMO_FEED_TABLE to dynamo feed table name'

    dynamo_client = boto3.client('dynamodb')
    dynamo_main_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_feed_table = boto3.resource('dynamodb').Table(DYNAMO_FEED_TABLE)

    migration = Migration(dynamo_client, dynamo_main_table, dynamo_feed_table)
    migration.run()

    return {'statusCode': 200, 'body': json.dumps('Migration completed successfully')}


if __name__ == '__main__':
    lambda_handler(None, None)
