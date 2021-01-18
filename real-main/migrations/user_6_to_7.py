import logging
import os

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "Set User.postArchivedCount"

    from_version = 6
    to_version = 7

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for user in self.generate_all_users_to_migrate():
            self.migrate_user(user)

    def generate_all_users_to_migrate(self):
        "Return a generator of all items in the table that pass the filter"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) and schemaVersion = :fsv',
            'ExpressionAttributeValues': {':pk_prefix': 'user/', ':fsv': self.from_version},
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
        logger.warning(f'User `{user_id}`: starting migration')
        org_count = user.get('postArchivedCount')
        actual_count = self.dynamo_count_archived_posts(user_id)
        self.dynamo_update_user(user_id, org_count, actual_count)
        logger.warning(f'User `{user_id}`: finished migration')

    def dynamo_update_user(self, user_id, org_count, actual_count):
        logger.warning(f'User `{user_id}`: updating user record with {actual_count} archived posts')
        kwargs = {
            'Key': {'partitionKey': f'user/{user_id}', 'sortKey': 'profile'},
            'UpdateExpression': 'SET schemaVersion = :tsv',
            'ConditionExpression': 'schemaVersion = :fsv',
            'ExpressionAttributeValues': {':tsv': self.to_version, ':fsv': self.from_version},
        }

        if actual_count == 0:
            kwargs['UpdateExpression'] += ' REMOVE postArchivedCount'
        else:
            kwargs['UpdateExpression'] += ', postArchivedCount = :pac'
            kwargs['ExpressionAttributeValues'][':pac'] = actual_count

        if org_count is None:
            kwargs['ConditionExpression'] += ' AND attribute_not_exists(postArchivedCount)'
        else:
            kwargs['ConditionExpression'] += ' AND postArchivedCount = :org_pac'
            kwargs['ExpressionAttributeValues'][':org_pac'] = org_count

        try:
            self.dynamo_table.update_item(**kwargs)
        except self.dynamo_client.exceptions.ConditionalCheckFailedException as err:
            raise Exception(
                f'Update failed for user `{user_id}` - post archived as migration ran? Run again'
            ) from err

    def dynamo_count_archived_posts(self, user_id):
        logger.warning(f'User `{user_id}`: counting archived posts')
        query_kwargs = {
            'KeyConditionExpression': (
                Key('gsiA2PartitionKey').eq(f'post/{user_id}') & Key('gsiA2SortKey').begins_with('ARCHIVED/')
            ),
            'IndexName': 'GSI-A2',
        }
        resp = self.dynamo_table.query(**query_kwargs)
        if resp.get('LastEvaluatedKey') is not None:
            raise Exception(f'Too many archived posts to count for user `{user_id}`')
        return len(resp['Items'])


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'
    dynamo_client = boto3.client('dynamodb')
    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
