import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "Fill User.followersRequestedCount"

    from_version = 9
    to_version = 10

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for user in self.generate_all_users_to_migrate():
            count = self.get_followers_requested_count(user)
            self.set_followers_requested_count(user, count)

    def generate_all_users_to_migrate(self):
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND sortKey = :sk AND schemaVersion = :sv',
            'ExpressionAttributeValues': {':pk_prefix': 'user/', ':sk': 'profile', ':sv': self.from_version},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def get_followers_requested_count(self, user):
        user_id = user['userId']
        query_kwargs = {
            'KeyConditionExpression': 'gsiA2PartitionKey = :pk AND begins_with(gsiA2SortKey, :sk_prefix)',
            'ExpressionAttributeValues': {':pk': f'followed/{user_id}', ':sk_prefix': 'REQUESTED/'},
            'IndexName': 'GSI-A2',
        }
        count = 0
        while True:
            paginated = self.dynamo_table.query(**query_kwargs)
            count += len(paginated['Items'])
            if 'LastEvaluatedKey' not in paginated:
                break
            query_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']
        return count

    def set_followers_requested_count(self, user, count):
        user_id = user['userId']
        kwargs = {
            'Key': {k: user[k] for k in ('partitionKey', 'sortKey')},
            'UpdateExpression': 'SET schemaVersion = :sv',
            'ConditionExpression': 'attribute_exists(partitionKey)',
            'ExpressionAttributeValues': {':sv': self.to_version},
        }

        if count > 0:
            kwargs['UpdateExpression'] += ', followersRequestedCount = :rfc'
            kwargs['ExpressionAttributeValues'][':rfc'] = count
        if count == 0:
            kwargs['UpdateExpression'] += ' REMOVE followersRequestedCount'

        org_count = user.get('followersRequestedCount')
        if org_count is None:
            kwargs['ConditionExpression'] += ' AND attribute_not_exists(followersRequestedCount)'
        else:
            kwargs['ConditionExpression'] += ' AND followersRequestedCount = :org_rfc'
            kwargs['ExpressionAttributeValues'][':org_rfc'] = org_count

        logger.warning(f'Migrating user `{user_id}`: setting followersRequestedCount to `{count}`')
        self.dynamo_table.update_item(**kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_client = boto3.client('dynamodb')
    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
