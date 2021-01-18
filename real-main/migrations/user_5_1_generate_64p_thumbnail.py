import io
import os

import boto3
import botocore
import PIL.Image
import PIL.ImageOps

S3_UPLOADS_BUCKET = os.environ.get('S3_UPLOADS_BUCKET')
if not S3_UPLOADS_BUCKET:
    raise Exception("Must set env variable S3_UPLOADS_BUCKET to bucket name")

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
if not DYNAMO_TABLE:
    raise Exception("Must set env variable DYNAMO_TABLE to dynamo table name")

boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

s3_client = boto3.client('s3')
s3_bucket = boto3.resource('s3').Bucket(S3_UPLOADS_BUCKET)


def generate_all_users_with_photo(version):
    "Return a generator of all items in the table that pass the filter"
    assert isinstance(version, int)
    scan_kwargs = {
        'FilterExpression': (
            'begins_with(partitionKey, :pk_prefix) and schemaVersion = :sv and attribute_exists(photoMediaId)'
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


def object_exists(path):
    # https://stackoverflow.com/a/33843019
    try:
        s3_bucket.Object(path).load()
    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == "404":
            return False
        raise
    return True


def put_object(path, body, content_type):
    print(f'Uploading object to {path} ...', end='')
    s3_bucket.put_object(Key=path, Body=body, ContentType=content_type)
    print(' done.')


def get_path(user_id, size, photo_media_id):
    filename = f'{size}.jpg'
    return '/'.join([user_id, 'profile-photo', photo_media_id, filename])


def get_data(path):
    return io.BytesIO(s3_bucket.Object(path).get()['Body'].read())


def generate_thumbnail(data, dims):
    image = PIL.Image.open(data)
    image = PIL.ImageOps.exif_transpose(image)
    image.thumbnail(dims)
    in_mem_file = io.BytesIO()
    image.save(in_mem_file, format='JPEG')
    in_mem_file.seek(0)
    return in_mem_file


def update_user(user_item):
    user_id = user_item['userId']
    photo_media_id = user_item['photoMediaId']

    native_path = get_path(user_id, 'native', photo_media_id)
    p64_path = get_path(user_id, '64p', photo_media_id)

    if object_exists(native_path) and not object_exists(p64_path):
        native_data = get_data(native_path)
        p64_data = generate_thumbnail(native_data, (114, 64))
        put_object(p64_path, p64_data, 'image/jpeg')


def main():
    for user_item in generate_all_users_with_photo(5):
        update_user(user_item)


main()
