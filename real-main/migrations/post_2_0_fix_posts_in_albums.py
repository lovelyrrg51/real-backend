import decimal
import logging
import os
import random

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    """
    For posts in albums with the 'old' GSI indexes, update them
    to the 'new' GSI indexes.

    Operates on posts with version 2, doesn't change the version.
    """

    from_version = 2

    def __init__(self, boto_client, boto_table):
        self.boto_client = boto_client
        self.boto_table = boto_table
        self.table_name = boto_table.name

    def run(self):
        for post in self.generate_all_posts_in_albums_without_new_key():
            self.migrate_post(post)

    def generate_all_posts_in_albums_without_new_key(self):
        "Return a generator of all items in the table that pass the filter"
        scan_kwargs = {
            'FilterExpression': (
                'begins_with(partitionKey, :pk_prefix)'
                ' and schemaVersion = :sv'
                ' and attribute_exists(albumId)'
                ' and attribute_not_exists(gsiK3PartitionKey)'
            ),
            'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sv': self.from_version},
        }
        while True:
            paginated = self.boto_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_post(self, post):
        post_id = post['postId']
        album_id = post['albumId']
        # we can rebalance the posts in the album later
        album_rank = decimal.Decimal(random.randrange(1, 1000 * 1000)) / 1000 / 1000 / 10
        kwargs = {
            'Key': {'partitionKey': f'post/{post_id}', 'sortKey': '-'},
            'UpdateExpression': (
                'SET gsiK3PartitionKey = :pk, gsiK3SortKey = :sk REMOVE gsiK2PartitionKey, gsiK2SortKey'
            ),
            'ConditionExpression': 'attribute_exists(partitionKey) and schemaVersion = :fromVersion',
            'ExpressionAttributeValues': {
                ':fromVersion': self.from_version,
                ':pk': f'post/{album_id}',
                ':sk': album_rank,
            },
        }
        logger.warning(f'Migrating post: `{post_id}`')
        self.update_item(kwargs)

    def update_item(self, kwargs):
        logger.info(f'Applying update_item with kwargs: {kwargs}')
        self.boto_table.update_item(**kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    boto_client = boto3.client('dynamodb')

    migration = Migration(boto_client, boto_table)
    migration.run()
