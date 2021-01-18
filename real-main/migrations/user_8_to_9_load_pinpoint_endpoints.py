import logging
import os
import uuid

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
PINPOINT_APPLICATION_ID = os.environ.get('PINPOINT_APPLICATION_ID')


class Migration:
    "Load up verified emails & phone numbers into pinpoint as endpoints"

    from_version = 8
    to_version = 9

    def __init__(self, dynamo_client, dynamo_table, pinpoint_client, pinpoint_app_id):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table
        self.pinpoint_client = pinpoint_client
        self.pinpoint_app_id = pinpoint_app_id

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
        if (email := user.get('email')) :
            self.pinpoint_update_endpoint(user_id, 'EMAIL', email)
        if (phone := user.get('phoneNumber')) :
            self.pinpoint_update_endpoint(user_id, 'SMS', phone)
        self.dynamo_update_user(user_id)

    def dynamo_update_user(self, user_id):
        logger.warning(f'User `{user_id}`: marking migrated')
        kwargs = {
            'Key': {'partitionKey': f'user/{user_id}', 'sortKey': 'profile'},
            'UpdateExpression': 'SET schemaVersion = :tsv',
            'ConditionExpression': 'schemaVersion = :fsv',
            'ExpressionAttributeValues': {':tsv': self.to_version, ':fsv': self.from_version},
        }
        self.dynamo_table.update_item(**kwargs)

    def pinpoint_update_endpoint(self, user_id, channel_type, address):
        endpoint_id = str(uuid.uuid4())
        logger.warning(
            f'User `{user_id}`: updating pinpoint endpoint `{endpoint_id}` type `{channel_type}` with `{address}`',
        )
        kwargs = {
            'ApplicationId': self.pinpoint_app_id,
            'EndpointId': endpoint_id,
            'EndpointRequest': {'Address': address, 'ChannelType': channel_type, 'User': {'UserId': user_id}},
        }
        self.pinpoint_client.update_endpoint(**kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'
    assert PINPOINT_APPLICATION_ID, 'Must set env variable PINPOINT_APPLICATION_ID to dynamo table name'

    dynamo_client = boto3.client('dynamodb')
    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    pinpoint_client = boto3.client('pinpoint')

    migration = Migration(dynamo_client, dynamo_table, pinpoint_client, PINPOINT_APPLICATION_ID)
    migration.run()
