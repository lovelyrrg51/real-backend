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
        for post_item in self.generate_posts_to_migrate():
            self.migrate_post(post_item)

    def generate_posts_to_migrate(self):
        "Return a generator of all posts that need to be migrated"
        scan_kwargs = {
            'FilterExpression': ' AND '.join(
                [
                    'begins_with(partitionKey, :pk_prefix)',
                    'verificationHidden = :true',
                    'attribute_exists(isVerified)',
                    'attribute_not_exists(isVerifiedHiddenValue)',
                ]
            ),
            'ExpressionAttributeValues': {':pk_prefix': 'post/', ':true': True},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_post(self, post_item):
        query_kwargs = {
            'Key': {k: post_item[k] for k in ('partitionKey', 'sortKey')},
            'UpdateExpression': 'SET isVerified = :true, isVerifiedHiddenValue = :org_iv',
            'ConditionExpression': ' AND '.join(
                ['attribute_exists(partitionKey)', 'isVerified = :org_iv', 'verificationHidden = :true'],
            ),
            'ExpressionAttributeValues': {':org_iv': post_item['isVerified'], ':true': True},
        }
        logger.warning(f'Migrating post `{post_item["postId"]}`')
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
