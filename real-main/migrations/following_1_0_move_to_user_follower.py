import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "Move following items to new user-child follower items"

    def __init__(self, boto_client, boto_table):
        self.boto_client = boto_client
        self.boto_table = boto_table
        self.table_name = boto_table.name

    def run(self):
        for key in self.generate_all_following_keys():
            typed_item = self.get_typed_item(key)
            self.move_following(typed_item)

    def generate_all_following_keys(self):
        "Return a generator of all items in the table that pass the filter"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
            'ExpressionAttributeValues': {':pk_prefix': 'following/'},
            'ProjectionExpression': 'partitionKey, sortKey',
        }
        while True:
            paginated = self.boto_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def get_typed_item(self, key):
        typed_key = {k: {'S': key[k]} for k in ('partitionKey', 'sortKey')}
        return self.boto_client.get_item(Key=typed_key, TableName=self.table_name).get('Item')

    def move_following(self, old_typed_item):
        old_typed_key = {k: old_typed_item[k] for k in ('partitionKey', 'sortKey')}
        new_typed_key = {
            'partitionKey': {'S': 'user/' + old_typed_item['followedUserId']['S']},
            'sortKey': {'S': 'follower/' + old_typed_item['followerUserId']['S']},
        }
        status = old_typed_item['followStatus']['S']

        transacts = [
            {
                'Put': {
                    'Item': {**old_typed_item, **new_typed_key},
                    'ConditionExpression': 'attribute_not_exists(partitionKey)',
                    'TableName': self.table_name,
                }
            },
            {
                'Delete': {
                    'Key': old_typed_key,
                    'ConditionExpression': 'attribute_exists(partitionKey) AND followStatus = :fs',
                    'ExpressionAttributeValues': {':fs': {'S': status}},
                    'TableName': self.table_name,
                }
            },
        ]

        logger.warning(f'Moving item from `{old_typed_key}` to `{new_typed_key}`')
        self.boto_client.transact_write_items(TransactItems=transacts)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    boto_client = boto3.client('dynamodb')

    migration = Migration(boto_client, boto_table)
    migration.run()
