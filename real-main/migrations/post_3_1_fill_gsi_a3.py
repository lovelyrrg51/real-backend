import logging
import os

import boto3
import pendulum

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "Fill Post.gsiA3PartitionKey and Post.gsiA3SortKey, delete Post.hasNewCommentActivity"

    attr = 'hasNewCommentActivity'
    gsiPk = 'gsiA3PartitionKey'
    gsiSk = 'gsiA3SortKey'

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table
        self.now = pendulum.now('utc')

    def run(self):
        for post in self.generate_all_posts_to_migrate():
            if post.get(self.attr, False):
                self.fill_gsi_and_remove_attr(post)
            else:
                self.remove_attr(post)

    def generate_all_posts_to_migrate(self):
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND attribute_exists(#attr)',
            'ExpressionAttributeNames': {'#attr': self.attr},
            'ExpressionAttributeValues': {':pk_prefix': 'post/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def fill_gsi_and_remove_attr(self, post):
        post_id = post['postId']
        logger.warning(f'Post `{post_id}`: removing {self.attr} and filling GSI-A3')
        user_id = post['postedByUserId']
        kwargs = {
            'Key': {'partitionKey': f'post/{post_id}', 'sortKey': '-'},
            'UpdateExpression': 'REMOVE #attr SET #gsiPk = :gsiPk, #gsiSk = :gsiSk',
            'ConditionExpression': 'attribute_exists(#attr)',
            'ExpressionAttributeNames': {'#attr': self.attr, '#gsiPk': self.gsiPk, '#gsiSk': self.gsiSk},
            'ExpressionAttributeValues': {':gsiPk': f'post/{user_id}', ':gsiSk': self.now.to_iso8601_string()},
        }
        self.dynamo_table.update_item(**kwargs)

    def remove_attr(self, post):
        post_id = post['postId']
        logger.warning(f'Post `{post_id}`: removing {self.attr}')
        kwargs = {
            'Key': {'partitionKey': f'post/{post_id}', 'sortKey': '-'},
            'UpdateExpression': 'REMOVE #attr',
            'ConditionExpression': 'attribute_exists(#attr)',
            'ExpressionAttributeNames': {'#attr': self.attr},
        }
        self.dynamo_table.update_item(**kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_client = boto3.client('dynamodb')
    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
