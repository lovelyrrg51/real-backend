import logging
import os

import boto3
from boto3.dynamodb.types import TypeSerializer

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Move Feed to be subitem of User"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table
        # https://stackoverflow.com/a/46738251
        self.serialize = TypeSerializer().serialize

    def run(self):
        for item in self.generate_all_old_feed_items():
            self.migrate_feed_item(item)

    def generate_all_old_feed_items(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
            'ExpressionAttributeValues': {':pk_prefix': 'feed/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_feed_item(self, item):
        old_key = {k: item[k] for k in ('partitionKey', 'sortKey')}
        user_id, post_id = item['userId'], item['postId']
        new_key = {'partitionKey': f'user/{user_id}', 'sortKey': f'feed/{post_id}'}
        new_item = {**item, **new_key}

        logger.warning(f'Migrating feed item for user `{user_id}` and post `{post_id}`')
        transacts = [
            {
                'Put': {
                    'Item': {k: self.serialize(v) for k, v in new_item.items()},
                    'ConditionExpression': 'attribute_not_exists(partitionKey)',
                    'TableName': self.dynamo_table.name,
                }
            },
            {
                'Delete': {
                    'Key': {k: self.serialize(v) for k, v in old_key.items()},
                    'ConditionExpression': 'attribute_exists(partitionKey)',
                    'TableName': self.dynamo_table.name,
                }
            },
        ]
        self.dynamo_client.transact_write_items(TransactItems=transacts)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
