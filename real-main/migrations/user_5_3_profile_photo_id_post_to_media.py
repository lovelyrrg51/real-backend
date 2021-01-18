import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    """
    Move User.photoMediaId to User.photoPostId.

    We don't try to translate from mediaId to postId because:
      - we would have to also move objects in S3
      - some of the original media will have been deleted, so it would
        end up being a best-effort translation anyway
    """

    def __init__(self, boto_table):
        self.boto_table = boto_table

    def run(self):
        for user_item in self.generate_all_users_with_photo_media_ids():
            self.migrate_user(user_item)

    def generate_all_users_with_photo_media_ids(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND attribute_exists(photoMediaId)',
            'ExpressionAttributeValues': {':pk_prefix': 'user/'},
        }
        while True:
            paginated = self.boto_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_user(self, user_item):
        user_id = user_item['userId']
        media_id = user_item['photoMediaId']
        logger.warning(f'Migrating `{user_id}`')

        kwargs = {
            'Key': {'partitionKey': user_item['partitionKey'], 'sortKey': user_item['sortKey']},
            'UpdateExpression': 'REMOVE photoMediaId SET photoPostId = :mid',
            'ExpressionAttributeValues': {':mid': media_id},
            'ConditionExpression': 'attribute_exists(partitionKey) AND photoMediaId = :mid',
        }

        self.boto_table.update_item(**kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'
    boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    migration = Migration(boto_table)
    migration.run()
