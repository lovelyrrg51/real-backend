import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    """
    Migrate posts from schemaVersion 1 to 2.

    Schema Version 2 adds the field `postType` to all post items.
    """

    from_version = 1
    to_version = 2

    def __init__(self, boto_client, boto_table):
        self.boto_client = boto_client
        self.boto_table = boto_table
        self.table_name = boto_table.name

    def run(self):
        for post in self.generate_all_posts():
            self.migrate_post(post)

    def generate_all_posts(self):
        "Return a generator of all items in the table that pass the filter"
        scan_kwargs = {
            'FilterExpression': ('begins_with(partitionKey, :pk_prefix) and schemaVersion = :sv'),
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
        # all posts in the DB up to this point are IMAGE posts
        post_type = 'IMAGE'
        post_id = post['postId']
        post_status = post['postStatus']
        posted_by_user_id = post['postedByUserId']
        posted_at_str = post['postedAt']
        kwargs = {
            'Key': {'partitionKey': f'post/{post_id}', 'sortKey': '-'},
            'UpdateExpression': (
                'SET postType = :pt, schemaVersion = :toVersion, gsiA3PartitionKey = :gsipk, gsiA3SortKey = :gsisk'
            ),
            'ConditionExpression': 'attribute_exists(partitionKey) and schemaVersion = :fromVersion',
            'ExpressionAttributeValues': {
                ':pt': post_type,
                ':fromVersion': self.from_version,
                ':toVersion': self.to_version,
                ':gsipk': f'post/{posted_by_user_id}',
                ':gsisk': f'{post_status}/{post_type}/{posted_at_str}',
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
