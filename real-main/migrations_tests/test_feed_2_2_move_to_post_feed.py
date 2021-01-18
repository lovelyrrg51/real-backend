import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.feed_2_2_move_to_post_feed import Migration


@pytest.fixture
def feed_item(dynamo_table):
    post_id = str(uuid4())
    posted_by_user_id = str(uuid4())
    user_id = str(uuid4())
    posted_at = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': f'feed/{post_id}',
        'schemaVersion': 2,
        'gsiA1PartitionKey': f'feed/{user_id}',
        'gsiA1SortKey': posted_at,
        'userId': user_id,
        'postId': post_id,
        'postedAt': posted_at,
        'postedByUserId': posted_by_user_id,
        'gsiK2PartitionKey': f'feed/{user_id}/{posted_by_user_id}',
        'gsiK2SortKey': posted_at,
    }
    dynamo_table.put_item(Item=item)
    yield item


f1 = feed_item
f2 = feed_item
f3 = feed_item
f4 = feed_item
f5 = feed_item
f6 = feed_item
f7 = feed_item
f8 = feed_item
f9 = feed_item
f10 = feed_item
f11 = feed_item
f12 = feed_item
f13 = feed_item


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog):
    # add something to the db to ensure it doesn't migrate
    pk = {'partitionKey': 'not-a-feed-item', 'sortKey': '-'}
    dynamo_table.put_item(Item=pk)
    assert dynamo_table.get_item(Key=pk)['Item'] == pk

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert dynamo_table.get_item(Key=pk)['Item'] == pk


def test_migrate_one(dynamo_client, dynamo_table, caplog, feed_item):
    user_id = feed_item['userId']
    post_id = feed_item['postId']

    old_pk = {'partitionKey': f'user/{user_id}', 'sortKey': f'feed/{post_id}'}
    new_pk = {'partitionKey': f'post/{post_id}', 'sortKey': f'feed/{user_id}'}

    # verify starting state
    old_item = dynamo_table.get_item(Key=old_pk)['Item']
    assert old_item
    assert 'Item' not in dynamo_table.get_item(Key=new_pk)

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in str(caplog.records[0])
    assert post_id in str(caplog.records[0])
    assert user_id in str(caplog.records[0])

    # verify final state
    assert 'Item' not in dynamo_table.get_item(Key=old_pk)
    new_item = dynamo_table.get_item(Key=new_pk)['Item']
    assert new_item.pop('partitionKey') == f'post/{post_id}'
    assert new_item.pop('sortKey') == f'feed/{user_id}'
    assert new_item.pop('gsiA2PartitionKey') == f'feed/{user_id}'
    assert new_item.pop('gsiA2SortKey') == old_item['postedByUserId']
    assert old_item.pop('partitionKey') == f'user/{user_id}'
    assert old_item.pop('sortKey') == f'feed/{post_id}'
    assert new_item == old_item


def test_migrate_twelve(dynamo_client, dynamo_table, caplog, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12):
    old_item_scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'user/', ':sk_prefix': 'feed/'},
    }
    new_item_scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sk_prefix': 'feed/'},
    }

    # check starting state
    assert len(dynamo_table.scan(**old_item_scan_kwargs)['Items']) == 12
    assert len(dynamo_table.scan(**new_item_scan_kwargs)['Items']) == 0

    # do the migration, check state
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    for f in (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12):
        assert f['userId'] in caplog.records[0].msg
        assert f['postId'] in caplog.records[0].msg
    assert len(dynamo_table.scan(**old_item_scan_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**new_item_scan_kwargs)['Items']) == 12

    # do the migration again, check is no-op
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert len(dynamo_table.scan(**old_item_scan_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**new_item_scan_kwargs)['Items']) == 12


def test_migrate_thirteen(
    dynamo_client, dynamo_table, caplog, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13
):
    old_item_scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'user/', ':sk_prefix': 'feed/'},
    }
    new_item_scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sk_prefix': 'feed/'},
    }

    # check starting state
    assert len(dynamo_table.scan(**old_item_scan_kwargs)['Items']) == 13
    assert len(dynamo_table.scan(**new_item_scan_kwargs)['Items']) == 0

    # do the migration, check state
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 2
    for f in (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12):
        assert f['userId'] in caplog.records[0].msg
        assert f['postId'] in caplog.records[0].msg
    for f in (f13,):
        assert f['userId'] in caplog.records[1].msg
        assert f['postId'] in caplog.records[1].msg
    assert len(dynamo_table.scan(**old_item_scan_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**new_item_scan_kwargs)['Items']) == 13

    # do the migration again, check is no-op
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert len(dynamo_table.scan(**old_item_scan_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**new_item_scan_kwargs)['Items']) == 13
