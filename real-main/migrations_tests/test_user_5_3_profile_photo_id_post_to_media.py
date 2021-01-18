import logging
import uuid

import pytest

from migrations.user_5_3_profile_photo_id_post_to_media import Migration

PK_KEYS = ('partitionKey', 'sortKey')


@pytest.fixture
def user_without_photo_media(dynamo_table):
    user_id = str(uuid.uuid4())
    # add to dynamo
    user_item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'userId': user_id,
    }
    dynamo_table.put_item(Item=user_item)
    yield user_item


@pytest.fixture
def user_with_photo_media(dynamo_table):
    user_id = str(uuid.uuid4())
    media_id = str(uuid.uuid4())
    # add to dynamo
    user_item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'photoMediaId': f'{media_id}',
        'userId': user_id,
    }
    dynamo_table.put_item(Item=user_item)
    yield user_item


user_with_photo_media_2 = user_with_photo_media


def test_nothing_to_migrate(dynamo_table, caplog, user_without_photo_media):
    pk = {k: v for k, v in user_without_photo_media.items() if k in PK_KEYS}

    # check starting state in dynamo
    item = dynamo_table.get_item(Key=pk)['Item']
    assert item == user_without_photo_media

    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # check no changes
    item = dynamo_table.get_item(Key=pk)['Item']
    assert item == user_without_photo_media


def test_migrate(dynamo_table, caplog, user_with_photo_media, user_with_photo_media_2):
    user_id_1 = user_with_photo_media['userId']
    user_id_2 = user_with_photo_media_2['userId']
    media_id_1 = user_with_photo_media['photoMediaId']
    media_id_2 = user_with_photo_media_2['photoMediaId']

    user_pk_1 = {k: v for k, v in user_with_photo_media.items() if k in PK_KEYS}
    user_pk_2 = {k: v for k, v in user_with_photo_media_2.items() if k in PK_KEYS}

    # check starting state dynamo
    item = dynamo_table.get_item(Key=user_pk_1)['Item']
    assert item['photoMediaId'] == media_id_1
    assert 'photoPhotoId' not in item
    item = dynamo_table.get_item(Key=user_pk_2)['Item']
    assert item['photoMediaId'] == media_id_2
    assert 'photoPhotoId' not in item

    # migrate
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # check logging worked
    assert len(caplog.records) == 2
    assert len([r for r in caplog.records if user_id_1 in str(r)]) == 1
    assert len([r for r in caplog.records if user_id_2 in str(r)]) == 1

    # check final state dynamo
    item = dynamo_table.get_item(Key=user_pk_1)['Item']
    assert item['photoPostId'] == media_id_1
    assert 'photoMediaId' not in item

    item = dynamo_table.get_item(Key=user_pk_2)['Item']
    assert item['photoPostId'] == media_id_2
    assert 'photoMediaId' not in item
