"""
Dynamo:
    - Remove email, phoneNumber, contactInfoSource, gsiA1PartitionKey and gsiA1SortKey
      for all users
"""
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
if not DYNAMO_TABLE:
    raise Exception("Must set env variable DYNAMO_TABLE to dynamo table name")

boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)


def generate_all_users(version):
    "Return a generator of all items in the table that pass the filter"
    assert isinstance(version, int)
    scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) and schemaVersion = :sv',
        'ExpressionAttributeValues': {':pk_prefix': 'user/', ':sv': version},
    }
    while True:
        paginated = boto_table.scan(**scan_kwargs)
        for item in paginated['Items']:
            yield item
        if 'LastEvaluatedKey' not in paginated:
            break
        scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']


def update_item(kwargs):
    print(f'Updating item: {kwargs} ... ')
    boto_table.update_item(**kwargs)
    print('Done.')


def update_user(user):
    kwargs = {
        'Key': {'partitionKey': user['partitionKey'], 'sortKey': user['sortKey']},
        'UpdateExpression': ' '.join(
            [
                'REMOVE email, phoneNumber, contactInfoSource, gsiA1PartitionKey, gsiA1SortKey',
                'SET schemaVersion = :three',
            ]
        ),
        'ConditionExpression': 'attribute_exists(partitionKey) and schemaVersion = :two',
        'ExpressionAttributeValues': {':two': 2, ':three': 3},
    }
    update_item(kwargs)


def main():
    for user in generate_all_users(2):
        update_user(user)


main()
