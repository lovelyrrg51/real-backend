import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
if not DYNAMO_TABLE:
    raise Exception("Must set env variable DYNAMO_TABLE to dynamo table name")

boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)


def generate_all_feed_items(version):
    "Return a generator of all items in the table that pass the filter"
    assert isinstance(version, int)
    scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) and schemaVersion = :sv',
        'ExpressionAttributeValues': {':pk_prefix': 'feed/', ':sv': version},
    }
    while True:
        paginated = boto_table.scan(**scan_kwargs)
        for item in paginated['Items']:
            yield item
        if 'LastEvaluatedKey' not in paginated:
            break
        scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']


def update_feed_item(item):
    user_id = item['partitionKey'].split('/')[1]
    posted_by_user_id = item['postedByUserId']
    set_exps = [
        'schemaVersion = :two',
        'userId = :uid',
        'gsiK2PartitionKey = :gsik2pk',
        'gsiK2SortKey = :gsik2sk',
    ]
    exp_values = {
        ':one': 1,
        ':two': 2,
        ':uid': user_id,
        ':gsik2pk': f'feed/{user_id}/{posted_by_user_id}',
        ':gsik2sk': item['postedAt'],
    }

    expires_at = item.get('expiresAt')
    if expires_at:
        set_exps.append('gsiK1PartitionKey = :gsik1pk')
        set_exps.append('gsiK1SortKey = :gsik1sk')
        exp_values[':gsik1pk'] = f'feed/{expires_at[:10]}'
        exp_values[':gsik1sk'] = expires_at[11:-1]

    kwargs = {
        'Key': {'partitionKey': item['partitionKey'], 'sortKey': item['sortKey']},
        'UpdateExpression': 'REMOVE #text SET ' + ', '.join(set_exps),
        'ConditionExpression': 'attribute_exists(partitionKey) and schemaVersion = :one',
        'ExpressionAttributeValues': exp_values,
        'ExpressionAttributeNames': {'#text': 'text'},
    }
    print(f'Updating item: {kwargs} ... ')
    boto_table.update_item(**kwargs)
    print('Done.')


def main():
    for item in generate_all_feed_items(1):
        update_feed_item(item)


main()
