"""
Dynamo:
    - add gsiA1PartitionKey, gsiA1SortKey for all users
"""
import os

import boto3

S3_UPLOADS_BUCKET = os.environ.get('S3_UPLOADS_BUCKET')
if not S3_UPLOADS_BUCKET:
    raise Exception("Must set env variable S3_UPLOADS_BUCKET to bucket name")

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
if not DYNAMO_TABLE:
    raise Exception("Must set env variable DYNAMO_TABLE to dynamo table name")

boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

s3_client = boto3.client('s3')
s3_bucket = boto3.resource('s3').Bucket(S3_UPLOADS_BUCKET)

media_sizes = (
    'native',
    '4K',
    '1080p',
    '480p',
)


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


def copy_object(old_path, new_path):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.copy
    print(f'Copying S3 object from {old_path} to {new_path} ...', end='')
    new_obj = s3_bucket.Object(new_path)
    new_obj.copy({'Bucket': S3_UPLOADS_BUCKET, 'Key': old_path})
    print(' done.')


def delete_object(old_path):
    print(f'Deleting S3 object from {old_path} ...', end='')
    old_obj = s3_bucket.Object(old_path)
    old_obj.delete()
    print(' done.')


def update_item(kwargs):
    print(f'Updating item: {kwargs} ... ')
    boto_table.update_item(**kwargs)
    print('Done.')


def get_path_old(user_id, size):
    filename = f'{size}.jpg'
    return '/'.join([user_id, 'profile-photo', filename])


def get_path_new(user_id, size, photo_media_id):
    filename = f'{size}.jpg'
    return '/'.join([user_id, 'profile-photo', photo_media_id, filename])


def update_user(user_item):
    user_id = user_item['userId']
    photo_media_id = user_item.get('photoMediaId')

    # copy the profile pics to their new location, delete the old ones
    if photo_media_id:
        for size in media_sizes:
            old_path = get_path_old(user_id, size)
            new_path = get_path_new(user_id, size, photo_media_id)
            copy_object(old_path, new_path)
            delete_object(old_path)

    # finally, update dynamo to indicate all done
    kwargs = {
        'Key': {'partitionKey': user_item['partitionKey'], 'sortKey': user_item['sortKey']},
        'UpdateExpression': ' '.join(['SET schemaVersion = :five']),
        'ConditionExpression': 'attribute_exists(partitionKey) and schemaVersion = :four',
        'ExpressionAttributeValues': {':four': 4, ':five': 5},
    }
    update_item(kwargs)


def main():
    for user_item in generate_all_users(4):
        update_user(user_item)


main()
