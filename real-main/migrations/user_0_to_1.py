"""
Dynamo:
    - Remove User.photoPath, add User.photoMediaId

S3:
    - Move user's native profile photo
    - Generate and store 480p, 1080p versions of user's profile photo
"""
import io
import os

import boto3
import PIL.Image

S3_UPLOADS_BUCKET = os.environ.get('S3_UPLOADS_BUCKET')
if not S3_UPLOADS_BUCKET:
    raise Exception("Must set env variable S3_UPLOADS_BUCKET to bucket name")

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
if not DYNAMO_TABLE:
    raise Exception("Must set env variable DYNAMO_TABLE to dynamo table name")

boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

s3_client = boto3.client('s3')
s3_bucket = boto3.resource('s3').Bucket(S3_UPLOADS_BUCKET)


def get_object_data_stream(path):
    return io.BytesIO(s3_bucket.Object(path).get()['Body'].read())


def copy_object(old_path, new_path):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.copy
    print(f'Copying S3 object from {old_path} to {new_path} ...', end='')
    new_obj = s3_bucket.Object(new_path)
    new_obj.copy({'Bucket': S3_UPLOADS_BUCKET, 'Key': old_path})
    print(' done.')


def put_object(path, body):
    print(f'Putting S3 object at {path} ...', end='')
    s3_bucket.put_object(Key=path, Body=body)
    print(' done.')


def delete_object(old_path):
    print(f'Deleting S3 object from {old_path} ...', end='')
    old_obj = s3_bucket.Object(old_path)
    old_obj.delete()
    print(' done.')


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


def update_dynamo(kwargs):
    print(f'Updating item: {kwargs} ... ')
    boto_table.update_item(**kwargs)
    print('Done.')


def update_user(user):
    # if we don't have a profile photo, we're done
    old_path = user.get('photoPath')
    if not old_path:
        kwargs = {
            'Key': {'partitionKey': user['partitionKey'], 'sortKey': user['sortKey']},
            'UpdateExpression': 'SET schemaVersion = :one',
            'ConditionExpression': 'attribute_exists(partitionKey) and schemaVersion = :zero',
            'ExpressionAttributeValues': {':zero': 0, ':one': 1},
        }
        update_dynamo(kwargs)
        return

    photo_media_id = old_path.split('/')[-1]

    # copy the native profile photo
    new_native_path = '/'.join([user['userId'], 'profile-photo', 'native.jpg'])
    copy_object(old_path, new_native_path)

    # generate the 480p thumbnail, upload it
    image = PIL.Image.open(get_object_data_stream(old_path))
    image.thumbnail([854, 480])
    in_mem_file = io.BytesIO()
    image.save(in_mem_file, format='JPEG')
    in_mem_file.seek(0)
    new_480p_path = '/'.join([user['userId'], 'profile-photo', '480p.jpg'])
    put_object(new_480p_path, in_mem_file.read())

    # generate the 1080p thumbnail, upload it
    image = PIL.Image.open(get_object_data_stream(old_path))
    image.thumbnail([1920, 1080])
    in_mem_file = io.BytesIO()
    image.save(in_mem_file, format='JPEG')
    in_mem_file.seek(0)
    new_480p_path = '/'.join([user['userId'], 'profile-photo', '1080p.jpg'])
    put_object(new_480p_path, in_mem_file.read())

    # update the DB
    kwargs = {
        'Key': {'partitionKey': user['partitionKey'], 'sortKey': user['sortKey']},
        'UpdateExpression': 'SET photoMediaId = :pmi, schemaVersion = :one REMOVE photoPath',
        'ConditionExpression': 'attribute_exists(partitionKey) and schemaVersion = :zero',
        'ExpressionAttributeValues': {':zero': 0, ':one': 1, ':pmi': photo_media_id},
    }
    update_dynamo(kwargs)

    # delete the old profile photo in s3
    delete_object(old_path)


def main():
    for user in generate_all_users(0):
        update_user(user)


main()
