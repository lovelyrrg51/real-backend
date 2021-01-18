import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Move Media.isVerified to Post.isVerified"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_all_media_objects_with_is_verified():
            self.migrate_media_object(item)

    def generate_all_media_objects_with_is_verified(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND attribute_exists(isVerified)',
            'ExpressionAttributeValues': {':pk_prefix': 'media/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_media_object(self, item):
        media_id = item['mediaId']
        logger.warning(f'Media `{media_id}`: starting migration')

        assert (post_id := item.get('postId'))
        assert (is_verified := item.get('isVerified')) is not None

        transact_media = {
            'Update': {
                'Key': {'partitionKey': {'S': f'media/{media_id}'}, 'sortKey': {'S': '-'}},
                'UpdateExpression': 'REMOVE isVerified',
                'ExpressionAttributeValues': {':iv': {'BOOL': is_verified}},
                'ConditionExpression': 'attribute_exists(partitionKey) AND isVerified = :iv',
                'TableName': self.dynamo_table.name,
            },
        }
        transact_post = {
            'Update': {
                'Key': {'partitionKey': {'S': f'post/{post_id}'}, 'sortKey': {'S': '-'}},
                'UpdateExpression': 'SET isVerified = :iv',
                'ExpressionAttributeValues': {':iv': {'BOOL': is_verified}},
                'ConditionExpression': 'attribute_exists(partitionKey) AND attribute_not_exists(isVerified)',
                'TableName': self.dynamo_table.name,
            },
        }
        self.dynamo_client.transact_write_items(TransactItems=[transact_post, transact_media])


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
