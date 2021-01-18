import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.like_1_1_move_to_post_like import Migration


@pytest.fixture
def like(dynamo_table):
    post_id = str(uuid4())
    posted_by_user_id = str(uuid4())
    liked_by_user_id = str(uuid4())
    liked_at_str = pendulum.now('utc').to_iso8601_string()
    like_status = 'the-like-status'
    item = {
        'partitionKey': f'like/{liked_by_user_id}/{post_id}',
        'sortKey': '-',
        'schemaVersion': 1,
        'gsiA1PartitionKey': f'like/{liked_by_user_id}',
        'gsiA1SortKey': f'{like_status}/{liked_at_str}',
        'gsiA2PartitionKey': f'like/{post_id}',
        'gsiA2SortKey': f'{like_status}/{liked_at_str}',
        'gsiK2PartitionKey': f'like/{posted_by_user_id}',
        'gsiK2SortKey': liked_by_user_id,
        'likedByUserId': liked_by_user_id,
        'likeStatus': like_status,
        'likedAt': liked_at_str,
        'postId': post_id,
    }
    dynamo_table.put_item(Item=item)
    yield item


like2 = like
like3 = like


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog):
    # add something to the db to ensure it doesn't migrate
    pk = {'partitionKey': 'not-a-like', 'sortKey': '-'}
    dynamo_table.put_item(Item=pk)
    assert dynamo_table.get_item(Key=pk)['Item'] == pk

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert dynamo_table.get_item(Key=pk)['Item'] == pk


def test_migrate_one(dynamo_client, dynamo_table, caplog, like):
    post_id = like['postId']
    liked_by_user_id = like['likedByUserId']

    like_pk = {'partitionKey': f'like/{liked_by_user_id}/{post_id}', 'sortKey': '-'}
    post_like_pk = {'partitionKey': f'post/{post_id}', 'sortKey': f'like/{liked_by_user_id}'}

    # verify starting state
    old_item = dynamo_table.get_item(Key=like_pk)['Item']
    assert old_item is not None
    assert 'Item' not in dynamo_table.get_item(Key=post_like_pk)

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert post_id in str(caplog.records[0])
    assert liked_by_user_id in str(caplog.records[0])

    # verify final state
    assert 'Item' not in dynamo_table.get_item(Key=like_pk)
    new_item = dynamo_table.get_item(Key=post_like_pk)['Item']
    assert new_item.pop('partitionKey') == f'post/{post_id}'
    assert new_item.pop('sortKey') == f'like/{liked_by_user_id}'
    assert old_item.pop('partitionKey') == f'like/{liked_by_user_id}/{post_id}'
    assert old_item.pop('sortKey') == '-'
    assert new_item == old_item


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, like, like2, like3):
    like_scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'like/'},
    }
    post_like_scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sk_prefix': 'like/'},
    }

    # check starting state
    assert len(dynamo_table.scan(**like_scan_kwargs)['Items']) == 3
    assert len(dynamo_table.scan(**post_like_scan_kwargs)['Items']) == 0

    # do the migration, check state
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert len(dynamo_table.scan(**like_scan_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**post_like_scan_kwargs)['Items']) == 3

    # do the migration again, check is no-op
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert len(dynamo_table.scan(**like_scan_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**post_like_scan_kwargs)['Items']) == 3
