import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "Delete Post.gsiA3PartitionKey and Post.gsiA3SortKey"

    gsiPk = 'gsiA3PartitionKey'
    gsiSk = 'gsiA3SortKey'

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for post in self.generate_all_posts_to_migrate():
            self.migrate_post(post)

    def generate_all_posts_to_migrate(self):
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND attribute_exists(#gsiPk)',
            'ExpressionAttributeNames': {'#gsiPk': self.gsiPk},
            'ExpressionAttributeValues': {':pk_prefix': 'post/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_post(self, post):
        post_id = post['postId']
        logger.warning(f'Post `{post_id}`: migrating')
        kwargs = {
            'Key': {'partitionKey': f'post/{post_id}', 'sortKey': '-'},
            'UpdateExpression': 'REMOVE #gsiPk, #gsiSk',
            'ConditionExpression': 'attribute_exists(partitionKey)',
            'ExpressionAttributeNames': {'#gsiPk': self.gsiPk, '#gsiSk': self.gsiSk},
        }
        self.dynamo_table.update_item(**kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_client = boto3.client('dynamodb')
    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
