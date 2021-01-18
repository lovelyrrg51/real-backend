import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Remove some excess fields from the firstStory item"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_all_to_migrate():
            self.migrate_item(item)

    def generate_all_to_migrate(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': ' AND '.join(
                [
                    'begins_with(partitionKey, :pk_prefix)',
                    'begins_with(sortKey, :sk_prefix)',
                    'contains(sortKey, :sk_suffix)',
                    'attribute_exists(postedByUserId)',
                ]
            ),
            'ExpressionAttributeValues': {
                ':pk_prefix': 'user/',
                ':sk_prefix': 'follower/',
                ':sk_suffix': '/firstStory',
            },
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_item(self, item):
        key = {k: item[k] for k in ('partitionKey', 'sortKey')}
        kwargs = {
            'Key': key,
            'UpdateExpression': 'REMOVE postedByUserId, gsiA1PartitionKey, gsiA1SortKey',
            'ConditionExpression': 'attribute_exists(partitionKey)',
        }
        logger.warning(f'Migrating firstStory `{key}`')
        self.dynamo_table.update_item(**kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
