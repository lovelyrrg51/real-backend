import json
import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    """
    Add attributes postId, gsiA2PartitionKey, gsiA2Sortkey to cards that are associated
    with posts.
    """

    version_from = 0
    version_to = 1

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_items_to_migrate():
            self.migrate_item(item)

    def generate_items_to_migrate(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND schemaVersion = :sv',
            'ExpressionAttributeValues': {':pk_prefix': 'card/', ':sv': self.version_from},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_item(self, item):
        card_id = item['partitionKey'].split('/')[1]
        card_id_parts = card_id.split(':')
        post_id = card_id_parts[3] if len(card_id_parts) > 3 else None
        user_id = ':'.join(card_id_parts[:2])

        query_kwargs = {
            'Key': {k: item[k] for k in ('partitionKey', 'sortKey')},
            'UpdateExpression': 'SET schemaVersion = :sv',
            'ConditionExpression': 'attribute_exists(partitionKey)',
            'ExpressionAttributeValues': {':sv': self.version_to},
        }
        if post_id:
            query_kwargs['UpdateExpression'] += ', postId = :pid, gsiA2PartitionKey = :pk, gsiA2SortKey = :sk'
            query_kwargs['ExpressionAttributeValues'][':pid'] = post_id
            query_kwargs['ExpressionAttributeValues'][':pk'] = f'card/{post_id}'
            query_kwargs['ExpressionAttributeValues'][':sk'] = user_id

        logger.warning(f'Migrating card `{card_id}`')
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
