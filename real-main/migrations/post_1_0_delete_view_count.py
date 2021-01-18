import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
if not DYNAMO_TABLE:
    raise Exception("Must set env variable DYNAMO_TABLE to dynamo table name")

boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)


def generate_all_posts_with_view_count(version):
    "Return a generator of all items in the table that pass the filter"
    assert isinstance(version, int)
    scan_kwargs = {
        'FilterExpression': (
            'begins_with(partitionKey, :pk_prefix) and schemaVersion = :sv and attribute_exists(viewCount)'
        ),
        'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sv': version},
    }
    while True:
        paginated = boto_table.scan(**scan_kwargs)
        for item in paginated['Items']:
            yield item
        if 'LastEvaluatedKey' not in paginated:
            break
        scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']


def update_post(post_item):
    post_id = post_item['postId']
    kwargs = {
        'Key': {'partitionKey': f'post/{post_id}', 'sortKey': '-'},
        'UpdateExpression': 'REMOVE viewCount',
        'ConditionExpression': 'attribute_exists(partitionKey) and schemaVersion = :one',
        'ExpressionAttributeValues': {':one': 1},
    }
    print(f'Updating item: {kwargs} ... ')
    boto_table.update_item(**kwargs)
    print('Done.')


def main():
    for post_item in generate_all_posts_with_view_count(1):
        update_post(post_item)


main()
