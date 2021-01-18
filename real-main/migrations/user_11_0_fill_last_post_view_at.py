import json
import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "For all trending items, remove the GSI-K3 index"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for post_view_item in self.generate_post_views():
            user_id = post_view_item['sortKey'].split('/')[1]
            at_str = post_view_item['lastViewedAt']
            self.update_user(user_id, at_str)

    def generate_post_views(self):
        "Return a generator of all post views that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
            'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sk_prefix': 'view/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def update_user(self, user_id, at_str):
        query_kwargs = {
            'Key': {'partitionKey': f'user/{user_id}', 'sortKey': 'profile'},
            'UpdateExpression': 'SET lastPostViewAt = :lpva',
            'ConditionExpression': 'attribute_exists(partitionKey) AND NOT lastPostViewAt >= :lpva',
            'ExpressionAttributeValues': {':lpva': at_str},
        }
        try:
            self.dynamo_table.update_item(**query_kwargs)
        except self.dynamo_client.exceptions.ConditionalCheckFailedException:
            logger.warning(f'User `{user_id}`: did not update')
        else:
            logger.warning(f'User `{user_id}`: updated')


def lambda_handler(event, context):
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()

    return {'statusCode': 200, 'body': json.dumps('Migration completed successfully')}


if __name__ == '__main__':
    lambda_handler(None, None)
