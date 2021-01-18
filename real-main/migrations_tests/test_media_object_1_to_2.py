import logging
import random
import string

import pytest

from migrations.media_object_1_to_2 import Migration

SIZES = ['native', '4K', '1080p', '480p', '64p']


@pytest.fixture(params=['UPLOADED', 'ARCHIVED'])
def media_object_with_s3(request, dynamo_table, s3_bucket):
    media_id = 'mid' + ''.join(random.choices(string.digits, k=4))
    user_id = 'uid' + ''.join(random.choices(string.digits, k=4))
    post_id = 'pid' + ''.join(random.choices(string.digits, k=4))
    # put placeholders in S3, both old and new paths
    for size in SIZES:
        old_path = f'{user_id}/post/{post_id}/media/{media_id}/{size}.jpg'
        new_path = f'{user_id}/post/{post_id}/image/{size}.jpg'
        s3_bucket.put_object(
            Key=old_path, Body=bytes(size, encoding='utf8'), ContentType='application/octet-stream'
        )
        s3_bucket.put_object(
            Key=new_path, Body=bytes(size, encoding='utf8'), ContentType='application/octet-stream'
        )
    # add to dynamo
    media_object = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'schemaVersion': 1,
        'mediaStatus': request.param,
        'userId': user_id,
        'postId': post_id,
        'mediaId': media_id,
    }
    dynamo_table.put_item(Item=media_object)
    yield media_object


@pytest.fixture(params=['AWAITING_UPLOAD', 'PROCESSING_UPLOAD', 'ERROR', 'DELETING'])
def media_object_without_s3(request, dynamo_table, s3_bucket):
    media_id = 'mid' + ''.join(random.choices(string.digits, k=4))
    user_id = 'uid' + ''.join(random.choices(string.digits, k=4))
    post_id = 'pid' + ''.join(random.choices(string.digits, k=4))
    # add to dynamo
    media_object = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'schemaVersion': 1,
        'mediaStatus': request.param,
        'userId': user_id,
        'postId': post_id,
        'mediaId': media_id,
    }
    dynamo_table.put_item(Item=media_object)
    yield media_object


def test_migrate_no_media_objects(dynamo_table, s3_bucket, caplog):
    migration = Migration(dynamo_table, s3_bucket)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0


def test_migrate_media_objects_no_s3_objects(dynamo_table, s3_bucket, caplog, media_object_without_s3):
    media_id = media_object_without_s3['mediaId']

    # check starting state dynamo
    item = dynamo_table.get_item(Key={'partitionKey': f'media/{media_id}', 'sortKey': '-'})['Item']
    assert item['schemaVersion'] == 1
    assert item == media_object_without_s3

    # migrate
    migration = Migration(dynamo_table, s3_bucket)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # check logging worked
    assert len(caplog.records) == 1
    assert media_id in str(caplog.records[0])
    assert 'dynamo' in str(caplog.records[0])

    # check final state dynamo, no changes
    item = dynamo_table.get_item(Key={'partitionKey': f'media/{media_id}', 'sortKey': '-'})['Item']
    assert item['schemaVersion'] == 2
    item['schemaVersion'] = 1
    assert item == media_object_without_s3


def test_migrate_media_object_old_s3_objects_deleted(dynamo_table, s3_bucket, caplog, media_object_with_s3):
    user_id = media_object_with_s3['userId']
    post_id = media_object_with_s3['postId']
    media_id = media_object_with_s3['mediaId']

    # check starting state s3
    for size in SIZES:
        old_path = f'{user_id}/post/{post_id}/media/{media_id}/{size}.jpg'
        old_data = s3_bucket.Object(old_path).get()['Body'].read()
        assert old_data == bytes(size, encoding='utf8')

        new_path = f'{user_id}/post/{post_id}/image/{size}.jpg'
        new_data = s3_bucket.Object(old_path).get()['Body'].read()
        assert new_data == bytes(size, encoding='utf8')

    # check starting state dynamo
    item = dynamo_table.get_item(Key={'partitionKey': f'media/{media_id}', 'sortKey': '-'})['Item']
    assert item['schemaVersion'] == 1
    assert item == media_object_with_s3

    # migrate
    migration = Migration(dynamo_table, s3_bucket)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # check logging worked
    assert len(caplog.records) == 6
    for rec in caplog.records:
        assert media_id in str(rec)
    for rec in caplog.records[:5]:
        assert 's3 delete' in str(rec)
    assert 'dynamo' in str(caplog.records[5])

    # check s3 deletes worked, just for the old paths
    for size in SIZES:
        old_path = f'{user_id}/post/{post_id}/media/{media_id}/{size}.jpg'
        with pytest.raises(s3_bucket.meta.client.exceptions.NoSuchKey):
            s3_bucket.Object(old_path).get()

        new_path = f'{user_id}/post/{post_id}/image/{size}.jpg'
        new_data = s3_bucket.Object(new_path).get()['Body'].read()
        assert new_data == bytes(size, encoding='utf8')

    # check dynamo schema version incremented
    item = dynamo_table.get_item(Key={'partitionKey': f'media/{media_id}', 'sortKey': '-'})['Item']
    assert item['schemaVersion'] == 2
    item['schemaVersion'] = 1
    assert item == media_object_with_s3


def test_migrate_multiple_media(dynamo_table, s3_bucket, caplog, media_object_with_s3, media_object_without_s3):
    media_id_with_s3 = media_object_with_s3['mediaId']
    media_id_without_s3 = media_object_without_s3['mediaId']

    # check starting state dynamo
    item = dynamo_table.get_item(Key={'partitionKey': f'media/{media_id_with_s3}', 'sortKey': '-'})['Item']
    assert item['schemaVersion'] == 1
    assert item == media_object_with_s3

    item = dynamo_table.get_item(Key={'partitionKey': f'media/{media_id_without_s3}', 'sortKey': '-'})['Item']
    assert item['schemaVersion'] == 1
    assert item == media_object_without_s3

    # migrate
    migration = Migration(dynamo_table, s3_bucket)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # check logging worked
    assert len(caplog.records) == 7

    # check final state dynamo
    item = dynamo_table.get_item(Key={'partitionKey': f'media/{media_id_with_s3}', 'sortKey': '-'})['Item']
    assert item['schemaVersion'] == 2
    item['schemaVersion'] = 1
    assert item == media_object_with_s3

    item = dynamo_table.get_item(Key={'partitionKey': f'media/{media_id_without_s3}', 'sortKey': '-'})['Item']
    assert item['schemaVersion'] == 2
    item['schemaVersion'] = 1
    assert item == media_object_without_s3
