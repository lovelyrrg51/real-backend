import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
if not DYNAMO_TABLE:
    raise Exception("Must set env variable DYNAMO_TABLE to dynamo table name")

boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)


def generate_all_users_with_post_view_count(version):
    "Return a generator of all items in the table that pass the filter"
    assert isinstance(version, int)
    scan_kwargs = {
        'FilterExpression': (
            'begins_with(partitionKey, :pk_prefix) and schemaVersion = :sv and attribute_exists(postViewCount)'
        ),
        'ExpressionAttributeValues': {':pk_prefix': 'user/', ':sv': version},
    }
    while True:
        paginated = boto_table.scan(**scan_kwargs)
        for item in paginated['Items']:
            yield item
        if 'LastEvaluatedKey' not in paginated:
            break
        scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']


def update_user(user_item):
    user_id = user_item['userId']
    kwargs = {
        'Key': {'partitionKey': f'user/{user_id}', 'sortKey': 'profile'},
        'UpdateExpression': 'REMOVE postViewCount',
        'ConditionExpression': 'attribute_exists(partitionKey) and schemaVersion = :five',
        'ExpressionAttributeValues': {':five': 5},
    }
    print(f'Updating item: {kwargs} ... ')
    boto_table.update_item(**kwargs)
    print('Done.')


def main():
    for user_item in generate_all_users_with_post_view_count(5):
        update_user(user_item)


main()
