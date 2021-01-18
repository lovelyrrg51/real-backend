import json
import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "For all trending items, copy the GSI-K3 index over to GSI-A4"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_items_to_migrate():
            self.migrate_item(item)

    def generate_items_to_migrate(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'sortKey = :sk AND attribute_not_exists(gsiA4PartitionKey)',
            'ExpressionAttributeValues': {':sk': 'trending'},
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
        query_kwargs = {
            'Key': key,
            'UpdateExpression': 'SET gsiA4PartitionKey = :pk, gsiA4SortKey = :sk',
            'ConditionExpression': 'gsiK3PartitionKey = :pk AND gsiK3SortKey = :sk',
            'ExpressionAttributeValues': {':pk': item['gsiK3PartitionKey'], ':sk': item['gsiK3SortKey']},
        }
        logger.warning(f'Migrating trending `{key}`')
        self.dynamo_table.update_item(**query_kwargs)


def lambda_handler(event, context):
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()

    return {'statusCode': 200, 'body': json.dumps('Migration completed successfully')}


if __name__ == '__main__':
    lambda_handler(None, None)
