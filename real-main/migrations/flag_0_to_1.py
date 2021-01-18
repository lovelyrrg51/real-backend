"""
Dynamo:
    - add gsiA1PartitionKey, gsiA1SortKey for all users
"""
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
if not DYNAMO_TABLE:
    raise Exception("Must set env variable DYNAMO_TABLE to dynamo table name")

boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)


def generate_all_flags(version):
    "Return a generator of all items in the table that pass the filter"
    assert isinstance(version, int)
    scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) and schemaVersion = :sv',
        'ExpressionAttributeValues': {':pk_prefix': 'flag/', ':sv': version},
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


def update_flag(flag):
    _, user_id, post_id = flag['partitionKey'].split('/')
    flagged_at = flag['likedAt']
    kwargs = {
        'Key': {'partitionKey': flag['partitionKey'], 'sortKey': flag['sortKey']},
        'UpdateExpression': ' '.join(
            [
                'SET '
                + ', '.join(
                    [
                        'gsiA1PartitionKey = :a1pk',
                        'gsiA1SortKey = :a1sk',
                        'gsiA2PartitionKey = :a2pk',
                        'gsiA2SortKey = :a2sk',
                        'postId = :pid',
                        'flaggerUserId = :fuid',
                        'flaggedAt = :fat',
                        'schemaVersion = :one',
                    ]
                )
                + ' REMOVE likedAt'
            ]
        ),
        'ConditionExpression': 'attribute_exists(partitionKey) and schemaVersion = :zero',
        'ExpressionAttributeValues': {
            ':zero': 0,
            ':one': 1,
            ':a1pk': f'flag/{user_id}',
            ':a1sk': flagged_at,
            ':a2pk': f'flag/{post_id}',
            ':a2sk': flagged_at,
            ':pid': post_id,
            ':fuid': user_id,
            ':fat': flagged_at,
        },
    }
    update_item(kwargs)


def main():
    for flag in generate_all_flags(0):
        update_flag(flag)


main()
