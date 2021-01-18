import io
import os

import boto3
import botocore
import colorthief

S3_UPLOADS_BUCKET = os.environ.get('S3_UPLOADS_BUCKET')
if not S3_UPLOADS_BUCKET:
    raise Exception("Must set env variable S3_UPLOADS_BUCKET to bucket name")

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
if not DYNAMO_TABLE:
    raise Exception("Must set env variable DYNAMO_TABLE to dynamo table name")

boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

s3_client = boto3.client('s3')
s3_bucket = boto3.resource('s3').Bucket(S3_UPLOADS_BUCKET)


def generate_all_media_objects_uploaded_or_archived(version):
    "Return a generator of all items in the table that pass the filter"
    assert isinstance(version, int)
    scan_kwargs = {
        'FilterExpression': (
            'begins_with(partitionKey, :pk_prefix)'
            ' and schemaVersion = :sv'
            ' and (mediaStatus = :u or mediaStatus = :a)'
            ' and attribute_not_exists(colors)'
        ),
        'ExpressionAttributeValues': {':pk_prefix': 'media/', ':sv': version, ':u': 'UPLOADED', ':a': 'ARCHIVED'},
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


def get_path(item, size):
    filename = f'{size}.jpg'
    return '/'.join([item['userId'], 'post', item['postId'], 'media', item['mediaId'], filename])


def get_data(path):
    return io.BytesIO(s3_bucket.Object(path).get()['Body'].read())


def get_colors(data):
    try:
        return colorthief.ColorThief(data).get_palette(color_count=5)
    except Exception:
        return None


def save_colors(media_item, colors):
    media_id = media_item['mediaId']
    color_maps = [{'r': ct[0], 'g': ct[1], 'b': ct[2]} for ct in colors]
    kwargs = {
        'Key': {'partitionKey': f'media/{media_id}', 'sortKey': '-'},
        'UpdateExpression': 'SET colors = :colors',
        'ExpressionAttributeValues': {':colors': color_maps},
        'ConditionExpression': 'attribute_exists(partitionKey) and attribute_not_exists(colors)',
    }
    print(f'Updating item: {kwargs} ... ')
    boto_table.update_item(**kwargs)
    print('Done.')


def update_media(item):
    media_id = item['mediaId']
    path = get_path(item, '480p')

    if object_exists(path):
        data = get_data(path)
        colors = get_colors(data)
        if colors:
            save_colors(item, colors)
        else:
            print(f'Unable to derive colors for media `{media_id}`')
    else:
        print(f'No s3 object found for media `{media_id}`')


def main():
    for item in generate_all_media_objects_uploaded_or_archived(0):
        update_media(item)


main()
