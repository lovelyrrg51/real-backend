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
        for item in self.generate_all_flags():
            self.migrate_flag(item)

    def generate_all_flags(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
            'ExpressionAttributeValues': {':pk_prefix': 'flag/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_flag(self, item):
        post_id = item['postId']
        user_id = item['flaggerUserId']
        at_str = item['flaggedAt']
        logger.warning(f'Flag for post `{post_id}` by user `{user_id}`: migrating')

        transact_add = {
            'Put': {
                'Item': {
                    'schemaVersion': {'N': '0'},
                    'partitionKey': {'S': f'post/{post_id}'},
                    'sortKey': {'S': f'flag/{user_id}'},
                    'gsiK1PartitionKey': {'S': f'flag/{user_id}'},
                    'gsiK1SortKey': {'S': '-'},
                    'createdAt': {'S': at_str},
                },
                'ConditionExpression': 'attribute_not_exists(partitionKey)',
                'TableName': self.dynamo_table.name,
            }
        }
        transact_delete = {
            'Delete': {
                'Key': {'partitionKey': {'S': f'flag/{user_id}/{post_id}'}, 'sortKey': {'S': '-'}},
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
