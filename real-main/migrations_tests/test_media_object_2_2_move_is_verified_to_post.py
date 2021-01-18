import logging
import uuid

import pytest

from migrations.media_object_2_2_move_is_verified_to_post import Migration


@pytest.fixture
def post_and_media_is_verified_none(dynamo_table):
    post_id = str(uuid.uuid4())
    post_item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
    }
    dynamo_table.put_item(Item=post_item)

    media_id = str(uuid.uuid4())
    media_item = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'mediaId': media_id,
        'postId': post_id,
    }
    dynamo_table.put_item(Item=media_item)
    yield post_item, media_item


@pytest.fixture
def post_and_media_is_verified_true(dynamo_table):
    post_id = str(uuid.uuid4())
    post_item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
    }
    dynamo_table.put_item(Item=post_item)

    media_id = str(uuid.uuid4())
    media_item = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'mediaId': media_id,
        'postId': post_id,
        'isVerified': True,
    }
    dynamo_table.put_item(Item=media_item)
    yield post_item, media_item


@pytest.fixture
def post_and_media_is_verified_false(dynamo_table):
    post_id = str(uuid.uuid4())
    post_item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
    }
    dynamo_table.put_item(Item=post_item)

    media_id = str(uuid.uuid4())
    media_item = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'mediaId': media_id,
        'postId': post_id,
        'isVerified': False,
    }
    dynamo_table.put_item(Item=media_item)
    yield post_item, media_item


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog, post_and_media_is_verified_none):
    post, media = post_and_media_is_verified_none

    post_id = post['postId']
    media_id = media['mediaId']

    post_pk = {'partitionKey': f'post/{post_id}', 'sortKey': '-'}
    media_pk = {'partitionKey': f'media/{media_id}', 'sortKey': '-'}

    # verify starting state
    assert dynamo_table.get_item(Key=post_pk)['Item'] == post
    assert dynamo_table.get_item(Key=media_pk)['Item'] == media

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify no change in db
    assert dynamo_table.get_item(Key=post_pk)['Item'] == post
    assert dynamo_table.get_item(Key=media_pk)['Item'] == media


def test_migrate_one(dynamo_client, dynamo_table, caplog, post_and_media_is_verified_false):
    post, media = post_and_media_is_verified_false

    post_id = post['postId']
    media_id = media['mediaId']

    post_pk = {'partitionKey': f'post/{post_id}', 'sortKey': '-'}
    media_pk = {'partitionKey': f'media/{media_id}', 'sortKey': '-'}

    # verify starting state
    assert dynamo_table.get_item(Key=post_pk)['Item'] == post
    assert dynamo_table.get_item(Key=media_pk)['Item'] == media

    assert 'isVerified' not in post
    assert media['isVerified'] is False

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert media_id in str(caplog.records[0])

    # verify item changed in db
    new_post = dynamo_table.get_item(Key=post_pk)['Item']
    new_media = dynamo_table.get_item(Key=media_pk)['Item']

    assert new_post['isVerified'] is False
    del new_post['isVerified']
    assert new_post == post

    assert 'isVerified' not in new_media
    new_media['isVerified'] = False
    assert new_media == media


def test_migrate_multiple(
    dynamo_client,
    dynamo_table,
    caplog,
    post_and_media_is_verified_false,
    post_and_media_is_verified_true,
    post_and_media_is_verified_none,
):
    post_false, media_false = post_and_media_is_verified_false
    post_true, media_true = post_and_media_is_verified_true
    post_none, _ = post_and_media_is_verified_none

    post_id_false = post_false['postId']
    post_id_true = post_true['postId']
    post_id_none = post_none['postId']

    media_id_false = media_false['mediaId']
    media_id_true = media_true['mediaId']

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 2
    assert media_id_false in str(caplog.records[0])
    assert media_id_true in str(caplog.records[1])

    # spot check result
    media_items = dynamo_table.scan(
        FilterExpression='begins_with(partitionKey, :pk_prefix)',
        ExpressionAttributeValues={':pk_prefix': 'media/'},
    )['Items']
    assert [m for m in media_items if 'isVerified' in m] == []

    post_items = dynamo_table.scan(
        FilterExpression='begins_with(partitionKey, :pk_prefix)',
        ExpressionAttributeValues={':pk_prefix': 'post/'},
    )['Items']
    assert [p['postId'] for p in post_items if p.get('isVerified') is True] == [post_id_true]
    assert [p['postId'] for p in post_items if p.get('isVerified') is False] == [post_id_false]
    assert [p['postId'] for p in post_items if p.get('isVerified') is None] == [post_id_none]
