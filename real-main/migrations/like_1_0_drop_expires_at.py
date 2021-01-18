import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
if not DYNAMO_TABLE:
    raise Exception("Must set env variable DYNAMO_TABLE to dynamo table name")

boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)


def generate_all_like_objects_with_expires_at():
    "Return a generator of all items in the table that pass the filter"
    scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) and attribute_exists(expiresAt)',
        'ExpressionAttributeValues': {':pk_prefix': 'like/'},
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


def update_like(item):
    kwargs = {
        'Key': {'partitionKey': item['partitionKey'], 'sortKey': item['sortKey']},
        'UpdateExpression': 'REMOVE expiresAt, gsiK1PartitionKey, gsiK1SortKey',
        'ConditionExpression': 'attribute_exists(partitionKey) and attribute_exists(expiresAt)',
    }
    update_item(kwargs)


def main():
    for item in generate_all_like_objects_with_expires_at():
        update_like(item)


main()
