import logging
import uuid

import pendulum
import pytest

from migrations.media_object_2_3_move_to_post_image import Migration


@pytest.fixture
def minimal_media(dynamo_table):
    user_id, post_id, media_id = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    item = {
        'schemaVersion': 2,
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'gsiA1PartitionKey': f'media/{post_id}',
        'gsiA1SortKey': '-',
        'userId': user_id,
        'postId': post_id,
        'postedAt': pendulum.now('utc').to_iso8601_string(),
        'mediaId': media_id,
        'mediaType': 'IMAGE',
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def maximal_media(dynamo_table):
    user_id, post_id, media_id = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    item = {
        'schemaVersion': 2,
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'gsiA1PartitionKey': f'media/{post_id}',
        'gsiA1SortKey': '-',
        'userId': user_id,
        'postId': post_id,
        'postedAt': pendulum.now('utc').to_iso8601_string(),
        'mediaId': media_id,
        'mediaType': 'IMAGE',
        'takenInReal': True,
        'originalFormat': 'HEIC',
        'imageFormat': 'HEIC',
        'height': 42,
        'width': 4242,
        'colors': [{'r': 1, 'g': 2, 'b': 3}, {'r': 4, 'g': 5, 'b': 6}, {'r': 7, 'g': 8, 'b': 9}],
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog):
    # add some distraction to the DB
    pk = {
        'partitionKey': 'post/' + str(uuid.uuid4()),
        'sortKey': 'image',
    }
    dynamo_table.put_item(Item=pk)

    # verify starting state
    assert dynamo_table.get_item(Key=pk)['Item'] == pk

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify no change in db
    assert dynamo_table.get_item(Key=pk)['Item'] == pk


def test_migrate_minimal(dynamo_client, dynamo_table, caplog, minimal_media):
    post_id, media_id = minimal_media['postId'], minimal_media['mediaId']
    post_image_pk = {'partitionKey': f'post/{post_id}', 'sortKey': 'image'}
    media_pk = {'partitionKey': f'media/{media_id}', 'sortKey': '-'}

    # verify starting state
    assert 'Item' not in dynamo_table.get_item(Key=post_image_pk)
    assert dynamo_table.get_item(Key=media_pk)['Item'] == minimal_media

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert media_id in str(caplog.records[0])

    # verify media item is gone from db
    assert 'Item' not in dynamo_table.get_item(Key=media_pk)

    # verify post image item now exists in DB, with correct form
    post_image = dynamo_table.get_item(Key=post_image_pk)['Item']
    assert post_image.pop('schemaVersion') == 0
    assert post_image == post_image_pk


def test_migrate_maximal(dynamo_client, dynamo_table, caplog, maximal_media):
    post_id, media_id = maximal_media['postId'], maximal_media['mediaId']
    post_image_pk = {'partitionKey': f'post/{post_id}', 'sortKey': 'image'}
    media_pk = {'partitionKey': f'media/{media_id}', 'sortKey': '-'}

    # verify starting state
    assert 'Item' not in dynamo_table.get_item(Key=post_image_pk)
    assert dynamo_table.get_item(Key=media_pk)['Item'] == maximal_media

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert media_id in str(caplog.records[0])

    # verify media item is gone from db
    assert 'Item' not in dynamo_table.get_item(Key=media_pk)

    # verify post image item now exists in DB, with correct form
    post_image = dynamo_table.get_item(Key=post_image_pk)['Item']
    assert post_image.pop('schemaVersion') == 0
    assert post_image.pop('takenInReal') == maximal_media['takenInReal']
    assert post_image.pop('originalFormat') == maximal_media['originalFormat']
    assert post_image.pop('imageFormat') == maximal_media['imageFormat']
    assert post_image.pop('height') == maximal_media['height']
    assert post_image.pop('width') == maximal_media['width']
    assert post_image.pop('colors') == maximal_media['colors']
    assert post_image == post_image_pk


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, minimal_media, maximal_media):
    post_id_1, media_id_1 = minimal_media['postId'], minimal_media['mediaId']
    post_id_2, media_id_2 = maximal_media['postId'], maximal_media['mediaId']

    post_image_pk_1 = {'partitionKey': f'post/{post_id_1}', 'sortKey': 'image'}
    post_image_pk_2 = {'partitionKey': f'post/{post_id_2}', 'sortKey': 'image'}

    media_pk_1 = {'partitionKey': f'media/{media_id_1}', 'sortKey': '-'}
    media_pk_2 = {'partitionKey': f'media/{media_id_2}', 'sortKey': '-'}

    # verify starting state
    assert 'Item' not in dynamo_table.get_item(Key=post_image_pk_1)
    assert 'Item' not in dynamo_table.get_item(Key=post_image_pk_2)
    assert dynamo_table.get_item(Key=media_pk_1)['Item'] == minimal_media
    assert dynamo_table.get_item(Key=media_pk_2)['Item'] == maximal_media

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 2
    assert media_id_1 in str(caplog.records[0])
    assert media_id_2 in str(caplog.records[1])

    # verify final state
    assert dynamo_table.get_item(Key=post_image_pk_1)['Item']['partitionKey'] == post_image_pk_1['partitionKey']
    assert dynamo_table.get_item(Key=post_image_pk_2)['Item']['partitionKey'] == post_image_pk_2['partitionKey']
    assert 'Item' not in dynamo_table.get_item(Key=media_pk_1)
    assert 'Item' not in dynamo_table.get_item(Key=media_pk_2)
