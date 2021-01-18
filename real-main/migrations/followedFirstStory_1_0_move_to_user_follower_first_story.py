import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Move the followedFirstStory item to user/follower/firstStory"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_all_to_migrate():
            self.migrate_item(item)

    def generate_all_to_migrate(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
            'ExpressionAttributeValues': {':pk_prefix': 'followedFirstStory/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_item(self, item):
        _, follower_user_id, followed_user_id = item['partitionKey'].split('/')
        transact_add = {
            'Put': {
                'Item': {
                    'schemaVersion': {'N': '1'},
                    'partitionKey': {'S': f'user/{followed_user_id}'},
                    'sortKey': {'S': f'follower/{follower_user_id}/firstStory'},
                    'gsiA1PartitionKey': {'S': item['gsiA1PartitionKey']},
                    'gsiA1SortKey': {'S': item['gsiA1SortKey']},
                    'gsiA2PartitionKey': {'S': f'follower/{follower_user_id}/firstStory'},
                    'gsiA2SortKey': {'S': item['expiresAt']},
                    'postedByUserId': {'S': item['postedByUserId']},
                    'postId': {'S': item['postId']},
                },
                'ConditionExpression': 'attribute_not_exists(partitionKey)',
                'TableName': self.dynamo_table.name,
            }
        }
        transact_delete = {
            'Delete': {
                'Key': {'partitionKey': {'S': item['partitionKey']}, 'sortKey': {'S': item['sortKey']}},
                'ConditionExpression': 'attribute_exists(partitionKey)',
                'TableName': self.dynamo_table.name,
            }
        }
        logger.warning(f'Migrating FFS for follower `{follower_user_id}` and followed `{followed_user_id}`')
        self.dynamo_client.transact_write_items(TransactItems=[transact_add, transact_delete])


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
