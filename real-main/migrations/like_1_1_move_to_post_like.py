import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Move Like item to be a sub-item of a Post"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_all_old_likes():
            self.migrate_like(item)

    def generate_all_old_likes(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
            'ExpressionAttributeValues': {':pk_prefix': 'like/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_like(self, item):
        post_id = item['postId']
        user_id = item['likedByUserId']
        logger.warning(f'Like for post `{post_id}` by user `{user_id}`: migrating')

        transact_add = {
            'Put': {
                'Item': {
                    'partitionKey': {'S': f'post/{post_id}'},
                    'sortKey': {'S': f'like/{user_id}'},
                    'schemaVersion': {'N': str(item['schemaVersion'])},
                    'gsiA1PartitionKey': {'S': item['gsiA1PartitionKey']},
                    'gsiA1SortKey': {'S': item['gsiA1SortKey']},
                    'gsiA2PartitionKey': {'S': item['gsiA2PartitionKey']},
                    'gsiA2SortKey': {'S': item['gsiA2SortKey']},
                    'gsiK2PartitionKey': {'S': item['gsiK2PartitionKey']},
                    'gsiK2SortKey': {'S': item['gsiK2SortKey']},
                    'likedByUserId': {'S': item['likedByUserId']},
                    'likeStatus': {'S': item['likeStatus']},
                    'likedAt': {'S': item['likedAt']},
                    'postId': {'S': item['postId']},
                },
                'ConditionExpression': 'attribute_not_exists(partitionKey)',
                'TableName': self.dynamo_table.name,
            }
        }
        transact_delete = {
            'Delete': {
                'Key': {'partitionKey': {'S': f'like/{user_id}/{post_id}'}, 'sortKey': {'S': '-'}},
                'ConditionExpression': 'attribute_exists(partitionKey)',
                'TableName': self.dynamo_table.name,
            }
        }
        self.dynamo_client.transact_write_items(TransactItems=[transact_add, transact_delete])


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
