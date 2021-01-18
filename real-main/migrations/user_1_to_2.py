"""
Dynamo:
    - Add gsiA1PartitionKey, gsiA1SortKey for all users with emails
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
    set_exps = ['schemaVersion = :two']
    exp_values = {
        ':one': 1,
        ':two': 2,
    }

    email = user.get('email')
    if email:
        set_exps.append('gsiA1PartitionKey = :ga1pk')
        set_exps.append('gsiA1SortKey = :ga1sk')
        exp_values[':ga1pk'] = f'userEmail/{email}'
        exp_values[':ga1sk'] = '-'

    kwargs = {
        'Key': {'partitionKey': user['partitionKey'], 'sortKey': user['sortKey']},
        'UpdateExpression': 'SET ' + ', '.join(set_exps),
        'ConditionExpression': 'attribute_exists(partitionKey) and schemaVersion = :one',
        'ExpressionAttributeValues': exp_values,
    }
    update_item(kwargs)


def main():
    for user in generate_all_users(1):
        update_user(user)


main()
