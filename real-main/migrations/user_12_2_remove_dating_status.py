import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "For all profile items, remove dating status to disable dating"

    attr_name = 'datingStatus'

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for user in self.generate_all_users_to_migrate():
            self.migrate_user(user)

    def generate_all_users_to_migrate(self):
        "Return a generator of all items in the table that pass the filter"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND attribute_exists(#attr_name)',
            'ExpressionAttributeNames': {'#attr_name': self.attr_name},
            'ExpressionAttributeValues': {':pk_prefix': 'user/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_user(self, user):
        user_id = user['userId']
        logger.warning(f'User `{user_id}`: migrating')
        kwargs = {
            'Key': {'partitionKey': f'user/{user_id}', 'sortKey': 'profile'},
            'UpdateExpression': 'REMOVE #attr_name',
            'ConditionExpression': 'attribute_exists(partitionKey)',
            'ExpressionAttributeNames': {'#attr_name': self.attr_name},
        }
        self.dynamo_table.update_item(**kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_client = boto3.client('dynamodb')
    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
