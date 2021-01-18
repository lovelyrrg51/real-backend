import logging
import os

import boto3
from boto3.dynamodb.types import TypeSerializer

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Move Feed to be subitem of Post"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table
        # https://stackoverflow.com/a/46738251
        self.serialize = TypeSerializer().serialize

    def run(self):
        group = []
        for item in self.generate_all_old_feed_items():
            group.append(item)
            # dynamo transactions allow up to 25 operations in a single transaction
            if len(group) >= 12:
                self.migrate_group(group)
                group = []
        if group:
            self.migrate_group(group)

    def generate_all_old_feed_items(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
            'ExpressionAttributeValues': {':pk_prefix': 'user/', ':sk_prefix': 'feed/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_group(self, group):
        transacts = []
        user_posts = []
        for item in group:
            old_key = {k: item[k] for k in ('partitionKey', 'sortKey')}
            user_id, post_id, posted_by_user_id = item['userId'], item['postId'], item['postedByUserId']
            new_key = {'partitionKey': f'post/{post_id}', 'sortKey': f'feed/{user_id}'}
            new_item = {
                **item,
                **new_key,
                'gsiA2PartitionKey': f'feed/{user_id}',
                'gsiA2SortKey': posted_by_user_id,
            }
            transacts.append(
                {
                    'Put': {
                        'Item': {k: self.serialize(v) for k, v in new_item.items()},
                        'ConditionExpression': 'attribute_not_exists(partitionKey)',
                        'TableName': self.dynamo_table.name,
                    }
                }
            )
            transacts.append(
                {
                    'Delete': {
                        'Key': {k: self.serialize(v) for k, v in old_key.items()},
                        'ConditionExpression': 'attribute_exists(partitionKey)',
                        'TableName': self.dynamo_table.name,
                    }
                }
            )
            user_posts.append([{'userId': user_id, 'postId': post_id}])

        logger.warning(f'Migrating {len(user_posts)} feed item(s) for users and posts: {user_posts}')
        self.dynamo_client.transact_write_items(TransactItems=transacts)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
