import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Move Media item to Post.Image item"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_all_media_objects():
            self.migrate_media_object(item)

    def generate_all_media_objects(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
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
        post_id = item['postId']
        media_id = item['mediaId']
        logger.warning(f'Media `{media_id}`: starting migration')

        transact_media = {
            'Delete': {
                'Key': {'partitionKey': {'S': f'media/{media_id}'}, 'sortKey': {'S': '-'}},
                'ConditionExpression': 'attribute_exists(partitionKey)',
                'TableName': self.dynamo_table.name,
            },
        }
        transact_post = {
            'Put': {
                'Item': {
                    'partitionKey': {'S': f'post/{post_id}'},
                    'sortKey': {'S': 'image'},
                    'schemaVersion': {'N': '0'},
                },
                'ConditionExpression': 'attribute_not_exists(partitionKey)',  # no updates, just adds
                'TableName': self.dynamo_table.name,
            },
        }
        if 'takenInReal' in item:
            transact_post['Put']['Item']['takenInReal'] = {'BOOL': item['takenInReal']}
        if 'originalFormat' in item:
            transact_post['Put']['Item']['originalFormat'] = {'S': item['originalFormat']}
        if 'imageFormat' in item:
            transact_post['Put']['Item']['imageFormat'] = {'S': item['imageFormat']}
        if 'height' in item:
            transact_post['Put']['Item']['height'] = {'N': str(item['height'])}
        if 'width' in item:
            transact_post['Put']['Item']['width'] = {'N': str(item['width'])}
        if 'colors' in item:
            transact_post['Put']['Item']['colors'] = {
                'L': [
                    {'M': {'r': {'N': str(color['r'])}, 'g': {'N': str(color['g'])}, 'b': {'N': str(color['b'])}}}
                    for color in item['colors']
                ]
            }
        self.dynamo_client.transact_write_items(TransactItems=[transact_post, transact_media])


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
