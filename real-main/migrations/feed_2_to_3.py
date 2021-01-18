import json
import logging
import os

import boto3
from boto3.dynamodb.types import TypeSerializer

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    """
    Clear a bunch of unused fields from feed items.

    Uses dynamo transactsions to batch PUT's with ConditionExpressions.
    Achieves about 6x speed-up when running from a lambda function.
    """

    version_from = 2
    version_to = 3

    carry_over_fields = (
        'partitionKey',
        'sortKey',
        'gsiA1PartitionKey',
        'gsiA1SortKey',
        'gsiA2PartitionKey',
        'gsiA2SortKey',
    )

    # dynamo transactions allow for up to 25 operations in a single transaction
    batch_size = 25

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table
        # https://stackoverflow.com/a/46738251
        self.serialize = TypeSerializer().serialize

    def run(self):
        group = []
        for item in self.generate_feed_items_to_migrate():
            group.append(item)
            if len(group) >= self.batch_size:
                self.migrate_group(group)
                group = []
        if group:
            self.migrate_group(group)

        for item in self.generate_feed_items_to_migrate():
            new_item = {**{k: item[k] for k in self.carry_over_fields}, 'schemaVersion': self.version_to}
            logger.warning(f'Migrating feed item {new_item["partitionKey"]} / {new_item["sortKey"]}')
            self.dynamo_table.put_item(Item=new_item, ConditionExpression='attribute_exists(partitionKey)')

    def generate_feed_items_to_migrate(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': ' AND '.join(
                [
                    'begins_with(partitionKey, :pk_prefix)',
                    'begins_with(sortKey, :sk_prefix)',
                    'schemaVersion = :sv',
                ]
            ),
            'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sk_prefix': 'feed/', ':sv': self.version_from},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_group(self, group):
        transacts = []
        keys = []
        for item in group:
            new_item = {**{k: item[k] for k in self.carry_over_fields}, 'schemaVersion': self.version_to}
            keys.append({k: item[k] for k in ('partitionKey', 'sortKey')})
            transacts.append(
                {
                    'Put': {
                        'Item': {k: self.serialize(v) for k, v in new_item.items()},
                        'ConditionExpression': 'attribute_exists(partitionKey)',
                        'TableName': self.dynamo_table.name,
                    }
                }
            )

        logger.warning(f'Migrating {len(keys)} feed item(s) for keys: {keys}')
        self.dynamo_client.transact_write_items(TransactItems=transacts)


def lambda_handler(event, context):
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()

    return {'statusCode': 200, 'body': json.dumps('Migration completed successfully')}


if __name__ == '__main__':
    lambda_handler(None, None)
