import logging
import os
import random
import string

import pytest

from migrations.post_2_to_3 import Migration

grant_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'grant.jpg')


@pytest.fixture
def image_data():
    with open(grant_path, 'rb') as fh:
        data = fh.read()
    yield data


@pytest.fixture
def post_with_native_image(dynamo_table, s3_bucket, image_data):
    user_id = 'uid' + ''.join(random.choices(string.digits, k=4))
    post_id = 'pid' + ''.join(random.choices(string.digits, k=4))
    native_path = f'{user_id}/post/{post_id}/image/native.jpg'
    s3_bucket.put_object(Key=native_path, Body=image_data, ContentType='image/jpeg')
    post_item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'schemaVersion': 2,
        'postedByUserId': user_id,
        'postId': post_id,
    }
    dynamo_table.put_item(Item=post_item)
    yield post_item


@pytest.fixture
def post_with_broken_image(dynamo_table, s3_bucket, image_data):
    user_id = 'uid' + ''.join(random.choices(string.digits, k=4))
    post_id = 'pid' + ''.join(random.choices(string.digits, k=4))
    native_path = f'{user_id}/post/{post_id}/image/native.jpg'
    s3_bucket.put_object(Key=native_path, Body='no an image', ContentType='image/jpeg')
    post_item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'schemaVersion': 2,
        'postedByUserId': user_id,
        'postId': post_id,
    }
    dynamo_table.put_item(Item=post_item)
    yield post_item


@pytest.fixture
def post_without_native_image(dynamo_table):
    user_id = 'uid' + ''.join(random.choices(string.digits, k=4))
    post_id = 'pid' + ''.join(random.choices(string.digits, k=4))
    post_item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'schemaVersion': 2,
        'postedByUserId': user_id,
        'postId': post_id,
    }
    dynamo_table.put_item(Item=post_item)
    yield post_item


def test_migrate_no_posts(dynamo_table, s3_bucket, caplog):
    migration = Migration(dynamo_table, s3_bucket)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0


def test_migrate_with_broken_image(dynamo_table, s3_bucket, caplog, post_with_broken_image):
    post_id = post_with_broken_image['postId']
    dynamo_pk = {'partitionKey': f'post/{post_id}', 'sortKey': '-'}
    assert dynamo_table.get_item(Key=dynamo_pk)['Item']['schemaVersion'] == 2

    migration = Migration(dynamo_table, s3_bucket)
    with caplog.at_level(logging.WARNING):
        migration.run()
    for rec in caplog.records:
        assert post_id in str(rec)
    assert len(caplog.records) == 3
    assert ' starting ' in str(caplog.records[0])
    assert ' s3: ' in str(caplog.records[1])
    assert ' dynamo: ' in str(caplog.records[2])

    assert dynamo_table.get_item(Key=dynamo_pk)['Item']['schemaVersion'] == 3


def test_migrate_with_native_image(dynamo_table, s3_bucket, caplog, post_with_native_image):
    post_id = post_with_native_image['postId']
    user_id = post_with_native_image['postedByUserId']
    dynamo_pk = {'partitionKey': f'post/{post_id}', 'sortKey': '-'}
    s3_native_path = f'{user_id}/post/{post_id}/image/native.jpg'
    s3_4k_path = f'{user_id}/post/{post_id}/image/4K.jpg'
    s3_1080p_path = f'{user_id}/post/{post_id}/image/1080p.jpg'
    s3_480p_path = f'{user_id}/post/{post_id}/image/480p.jpg'
    s3_64p_path = f'{user_id}/post/{post_id}/image/64p.jpg'

    # verify starting state
    assert dynamo_table.get_item(Key=dynamo_pk)['Item']['schemaVersion'] == 2
    s3_bucket.Object(s3_native_path).get()
    with pytest.raises(s3_bucket.meta.client.exceptions.NoSuchKey):
        s3_bucket.Object(s3_4k_path).get()
    with pytest.raises(s3_bucket.meta.client.exceptions.NoSuchKey):
        s3_bucket.Object(s3_1080p_path).get()
    with pytest.raises(s3_bucket.meta.client.exceptions.NoSuchKey):
        s3_bucket.Object(s3_480p_path).get()
    with pytest.raises(s3_bucket.meta.client.exceptions.NoSuchKey):
        s3_bucket.Object(s3_64p_path).get()

    # migrate
    migration = Migration(dynamo_table, s3_bucket)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # verify logging worked
    assert len(caplog.records) == 6
    for rec in caplog.records:
        assert post_id in str(rec)
    assert ' starting ' in str(caplog.records[0])
    assert ' s3: ' in str(caplog.records[1])
    assert ' s3: ' in str(caplog.records[2])
    assert ' s3: ' in str(caplog.records[3])
    assert ' s3: ' in str(caplog.records[4])
    assert ' dynamo: ' in str(caplog.records[5])
    assert '/image/4K.jpg' in str(caplog.records[1])
    assert '/image/1080p.jpg' in str(caplog.records[2])
    assert '/image/480p.jpg' in str(caplog.records[3])
    assert '/image/64p.jpg' in str(caplog.records[4])

    # verify final state
    assert dynamo_table.get_item(Key=dynamo_pk)['Item']['schemaVersion'] == 3
    s3_bucket.Object(s3_native_path).get()
    s3_bucket.Object(s3_4k_path).get()
    s3_bucket.Object(s3_1080p_path).get()
    s3_bucket.Object(s3_480p_path).get()
    s3_bucket.Object(s3_64p_path).get()


def test_migrate_multiple(dynamo_table, s3_bucket, caplog, post_with_native_image, post_without_native_image):
    post_id_1 = post_with_native_image['postId']
    post_id_2 = post_without_native_image['postId']
    dynamo_pk_1 = {'partitionKey': f'post/{post_id_1}', 'sortKey': '-'}
    dynamo_pk_2 = {'partitionKey': f'post/{post_id_2}', 'sortKey': '-'}

    # verify starting state
    assert dynamo_table.get_item(Key=dynamo_pk_1)['Item']['schemaVersion'] == 2
    assert dynamo_table.get_item(Key=dynamo_pk_2)['Item']['schemaVersion'] == 2

    # migrate
    migration = Migration(dynamo_table, s3_bucket)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # verify logging worked
    assert len(caplog.records) == 8
    assert len([r for r in caplog.records if post_id_1 in str(r)]) == 6
    assert len([r for r in caplog.records if post_id_2 in str(r)]) == 2

    # verify final state
    assert dynamo_table.get_item(Key=dynamo_pk_1)['Item']['schemaVersion'] == 3
    assert dynamo_table.get_item(Key=dynamo_pk_2)['Item']['schemaVersion'] == 3
