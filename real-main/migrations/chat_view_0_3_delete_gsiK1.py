import json
import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "Remove GSI-K1 from chat views"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for cv in self.generate_all_chat_views():
            self.migrate_chat_view(cv)

    def generate_all_chat_views(self):
        "Return a generator of all items in the table that pass the filter"
        scan_kwargs = {
            'FilterExpression': ' AND '.join(
                [
                    'begins_with(partitionKey, :pk_prefix)',
                    'begins_with(sortKey, :sk_prefix)',
                    'attribute_exists(gsiK1PartitionKey)',
                ]
            ),
            'ExpressionAttributeValues': {':pk_prefix': 'chat/', ':sk_prefix': 'view/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_chat_view(self, cv):
        key = {k: cv[k] for k in ('partitionKey', 'sortKey')}
        kwargs = {
            'Key': key,
            'UpdateExpression': 'REMOVE gsiK1PartitionKey, gsiK1SortKey',
            'ConditionExpression': 'attribute_exists(partitionKey)',
        }
        logger.warning(f'Migrating chat view: `{key}`')
        self.dynamo_table.update_item(**kwargs)


def lambda_handler(event, context):
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()

    return {'statusCode': 200, 'body': json.dumps('Migration completed successfully')}


if __name__ == '__main__':
    lambda_handler(None, None)
