import json
import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "Add GSI-A1 and GSI-A2 to post views"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for pv in self.generate_all_post_views():
            self.migrate_post_view(pv)

    def generate_all_post_views(self):
        "Return a generator of all items in the table that pass the filter"
        scan_kwargs = {
            'FilterExpression': ' AND '.join(
                [
                    'begins_with(partitionKey, :pk_prefix)',
                    'begins_with(sortKey, :sk_prefix)',
                    'attribute_not_exists(gsiA1PartitionKey)',
                ]
            ),
            'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sk_prefix': 'view/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_post_view(self, pv):
        post_id = pv['partitionKey'].split('/')[1]
        user_id = pv['sortKey'].split('/')[1]
        first_viewed_at = pv['firstViewedAt']
        kwargs = {
            'Key': {k: pv[k] for k in ('partitionKey', 'sortKey')},
            'UpdateExpression': 'SET #a1pk = :a1pk, #a1sk = :a1sk, #a2pk = :a2pk, #a2sk = :a2sk',
            'ConditionExpression': 'attribute_exists(partitionKey)',
            'ExpressionAttributeNames': {
                '#a1pk': 'gsiA1PartitionKey',
                '#a1sk': 'gsiA1SortKey',
                '#a2pk': 'gsiA2PartitionKey',
                '#a2sk': 'gsiA2SortKey',
            },
            'ExpressionAttributeValues': {
                ':a1pk': f'postView/{post_id}',
                ':a1sk': first_viewed_at,
                ':a2pk': f'postView/{user_id}',
                ':a2sk': first_viewed_at,
            },
        }
        logger.warning(f'Migrating post view for post `{post_id}` and user `{user_id}`')
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
