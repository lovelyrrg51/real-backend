import json
import logging
import os

import boto3
from boto3.dynamodb.types import TypeSerializer

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Move Block item to be a sub-item of a User"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table
        self.serialize = TypeSerializer().serialize

    def run(self):
        for item in self.generate_all_items_to_migrate():
            self.migrate_item(item)

    def generate_all_items_to_migrate(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
            'ExpressionAttributeValues': {':pk_prefix': 'block/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_item(self, item):
        blocked_user_id = item['blockedUserId']
        blocker_user_id = item['blockerUserId']
        old_key = {k: item[k] for k in ('partitionKey', 'sortKey')}
        new_key = {'partitionKey': f'user/{blocked_user_id}', 'sortKey': f'blocker/{blocker_user_id}'}
        new_item = {**item, **new_key}

        transact_add = {
            'Put': {
                'Item': {k: self.serialize(v) for k, v in new_item.items()},
                'ConditionExpression': 'attribute_not_exists(partitionKey)',
                'TableName': self.dynamo_table.name,
            }
        }
        transact_delete = {
            'Delete': {
                'Key': {k: self.serialize(v) for k, v in old_key.items()},
                'ConditionExpression': 'attribute_exists(partitionKey)',
                'TableName': self.dynamo_table.name,
            }
        }
        logger.warning(f'Migrating block for users `{blocked_user_id}` and `{blocker_user_id}`')
        self.dynamo_client.transact_write_items(TransactItems=[transact_add, transact_delete])


def lambda_handler(event, context):
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()

    return {'statusCode': 200, 'body': json.dumps('Migration completed successfully')}


if __name__ == '__main__':
    lambda_handler(None, None)
