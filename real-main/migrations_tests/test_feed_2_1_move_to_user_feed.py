import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.feed_2_1_move_to_user_feed import Migration


@pytest.fixture
def feed_item(dynamo_table):
    post_id = str(uuid4())
    posted_by_user_id = str(uuid4())
    user_id = str(uuid4())
    posted_at = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': f'feed/{user_id}/{post_id}',
        'sortKey': '-',
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


feed_item2 = feed_item
feed_item3 = feed_item


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

    old_pk = {'partitionKey': f'feed/{user_id}/{post_id}', 'sortKey': '-'}
    new_pk = {'partitionKey': f'user/{user_id}', 'sortKey': f'feed/{post_id}'}

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
    assert new_item.pop('partitionKey') == f'user/{user_id}'
    assert new_item.pop('sortKey') == f'feed/{post_id}'
    assert old_item.pop('partitionKey') == f'feed/{user_id}/{post_id}'
    assert old_item.pop('sortKey') == '-'
    assert new_item == old_item


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, feed_item, feed_item2, feed_item3):
    old_item_scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'feed/'},
    }
    new_item_scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'user/', ':sk_prefix': 'feed/'},
    }

    # check starting state
    assert len(dynamo_table.scan(**old_item_scan_kwargs)['Items']) == 3
    assert len(dynamo_table.scan(**new_item_scan_kwargs)['Items']) == 0

    # do the migration, check state
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert len(dynamo_table.scan(**old_item_scan_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**new_item_scan_kwargs)['Items']) == 3

    # do the migration again, check is no-op
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert len(dynamo_table.scan(**old_item_scan_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**new_item_scan_kwargs)['Items']) == 3
