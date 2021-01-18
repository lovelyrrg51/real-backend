import json
import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Add user subitems of userEmail, userPhoneNumber"

    version_from = 10
    version_to = 11

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_items_to_migrate():
            self.migrate_item(item)

    def generate_items_to_migrate(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND schemaVersion = :sv',
            'ExpressionAttributeValues': {':pk_prefix': 'user/', ':sv': self.version_from},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_item(self, item):
        user_id = item['userId']
        ops = ['update User.schemaVersion']
        transacts = [
            {
                'Update': {
                    'Key': {'partitionKey': {'S': f'user/{user_id}'}, 'sortKey': {'S': 'profile'}},
                    'UpdateExpression': 'SET schemaVersion = :sv',
                    'ConditionExpression': 'attribute_exists(partitionKey)',
                    'ExpressionAttributeValues': {':sv': {'N': str(self.version_to)}},
                    'TableName': self.dynamo_table.name,
                }
            }
        ]
        if email := item.get('email'):
            ops.append('add userEmail')
            transacts.append(
                {
                    'Put': {
                        'Item': {
                            'partitionKey': {'S': f'userEmail/{email}'},
                            'sortKey': {'S': '-'},
                            'schemaVersion': {'N': '0'},
                            'userId': {'S': user_id},
                        },
                        'TableName': self.dynamo_table.name,
                    }
                }
            )
        if phone_number := item.get('phoneNumber'):
            ops.append('add userPhoneNumber')
            transacts.append(
                {
                    'Put': {
                        'Item': {
                            'partitionKey': {'S': f'userPhoneNumber/{phone_number}'},
                            'sortKey': {'S': '-'},
                            'schemaVersion': {'N': '0'},
                            'userId': {'S': user_id},
                        },
                        'TableName': self.dynamo_table.name,
                    }
                }
            )
        logger.warning(f'Migrating user `{user_id}`: {", ".join(ops)}')
        self.dynamo_client.transact_write_items(TransactItems=transacts)


def lambda_handler(event, context):
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()

    return {'statusCode': 200, 'body': json.dumps('Migration completed successfully')}


if __name__ == '__main__':
    lambda_handler(None, None)
