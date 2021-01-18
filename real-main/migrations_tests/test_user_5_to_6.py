import logging
import os
import random
import string

import pytest

from migrations.user_5_to_6 import Migration

grant_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'grant.jpg')


@pytest.fixture
def image_data():
    with open(grant_path, 'rb') as fh:
        data = fh.read()
    yield data


@pytest.fixture
def user_with_photo(dynamo_table, s3_bucket, image_data):
    user_id = 'uid' + ''.join(random.choices(string.digits, k=4))
    post_id = 'pid' + ''.join(random.choices(string.digits, k=4))
    native_path = f'{user_id}/profile-photo/{post_id}/native.jpg'
    s3_bucket.put_object(Key=native_path, Body=image_data, ContentType='image/jpeg')
    user_item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 5,
        'userId': user_id,
        'photoPostId': post_id,
    }
    dynamo_table.put_item(Item=user_item)
    yield user_item


@pytest.fixture
def user_with_broken_photo(dynamo_table, s3_bucket, image_data):
    user_id = 'uid' + ''.join(random.choices(string.digits, k=4))
    post_id = 'pid' + ''.join(random.choices(string.digits, k=4))
    native_path = f'{user_id}/profile-photo/{post_id}/native.jpg'
    s3_bucket.put_object(Key=native_path, Body='not image data', ContentType='image/jpeg')
    user_item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 5,
        'userId': user_id,
        'photoPostId': post_id,
    }
    dynamo_table.put_item(Item=user_item)
    yield user_item


@pytest.fixture
def user_without_photo(dynamo_table):
    user_id = 'uid' + ''.join(random.choices(string.digits, k=4))
    user_item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 5,
        'userId': user_id,
    }
    dynamo_table.put_item(Item=user_item)
    yield user_item


def test_migrate_no_users(dynamo_table, s3_bucket, caplog):
    migration = Migration(dynamo_table, s3_bucket)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0


def test_migrate_with_photo(dynamo_table, s3_bucket, caplog, user_with_photo):
    user_id = user_with_photo['userId']
    post_id = user_with_photo['photoPostId']
    dynamo_pk = {'partitionKey': f'user/{user_id}', 'sortKey': 'profile'}
    s3_native_path = f'{user_id}/profile-photo/{post_id}/native.jpg'
    s3_4k_path = f'{user_id}/profile-photo/{post_id}/4K.jpg'
    s3_1080p_path = f'{user_id}/profile-photo/{post_id}/1080p.jpg'
    s3_480p_path = f'{user_id}/profile-photo/{post_id}/480p.jpg'
    s3_64p_path = f'{user_id}/profile-photo/{post_id}/64p.jpg'

    # verify starting state
    assert dynamo_table.get_item(Key=dynamo_pk)['Item']['schemaVersion'] == 5
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
        assert user_id in str(rec)
    assert ' starting ' in str(caplog.records[0])
    assert ' s3: ' in str(caplog.records[1])
    assert ' s3: ' in str(caplog.records[2])
    assert ' s3: ' in str(caplog.records[3])
    assert ' s3: ' in str(caplog.records[4])
    assert ' dynamo: ' in str(caplog.records[5])
    assert '/4K.jpg' in str(caplog.records[1])
    assert '/1080p.jpg' in str(caplog.records[2])
    assert '/480p.jpg' in str(caplog.records[3])
    assert '/64p.jpg' in str(caplog.records[4])

    # verify final state
    assert dynamo_table.get_item(Key=dynamo_pk)['Item']['schemaVersion'] == 6
    s3_bucket.Object(s3_native_path).get()
    s3_bucket.Object(s3_4k_path).get()
    s3_bucket.Object(s3_1080p_path).get()
    s3_bucket.Object(s3_480p_path).get()
    s3_bucket.Object(s3_64p_path).get()


def test_migrate_with_broken_photo(dynamo_table, s3_bucket, caplog, user_with_broken_photo):
    user_id = user_with_broken_photo['userId']
    dynamo_pk = {'partitionKey': f'user/{user_id}', 'sortKey': 'profile'}

    # verify starting state
    assert dynamo_table.get_item(Key=dynamo_pk)['Item']['schemaVersion'] == 5

    # migrate
    migration = Migration(dynamo_table, s3_bucket)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # verify logging worked
    assert len(caplog.records) == 3
    for rec in caplog.records:
        assert user_id in str(rec)
    assert ' starting ' in str(caplog.records[0])
    assert ' s3: ' in str(caplog.records[1])
    assert ' dynamo: ' in str(caplog.records[2])

    # verify final state
    assert dynamo_table.get_item(Key=dynamo_pk)['Item']['schemaVersion'] == 6


def test_migrate_multiple(dynamo_table, s3_bucket, caplog, user_with_photo, user_without_photo):
    user_id_1 = user_with_photo['userId']
    user_id_2 = user_without_photo['userId']
    dynamo_pk_1 = {'partitionKey': f'user/{user_id_1}', 'sortKey': 'profile'}
    dynamo_pk_2 = {'partitionKey': f'user/{user_id_2}', 'sortKey': 'profile'}

    # verify starting state
    assert dynamo_table.get_item(Key=dynamo_pk_1)['Item']['schemaVersion'] == 5
    assert dynamo_table.get_item(Key=dynamo_pk_2)['Item']['schemaVersion'] == 5

    # migrate
    migration = Migration(dynamo_table, s3_bucket)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # verify logging worked
    assert len(caplog.records) == 8
    assert len([r for r in caplog.records if user_id_1 in str(r)]) == 6
    assert len([r for r in caplog.records if user_id_2 in str(r)]) == 2

    # verify final state
    assert dynamo_table.get_item(Key=dynamo_pk_1)['Item']['schemaVersion'] == 6
    assert dynamo_table.get_item(Key=dynamo_pk_2)['Item']['schemaVersion'] == 6
