import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.feed_3_0_copy_to_feed_table import Migration


@pytest.fixture
def feed_item(dynamo_table):
    post_id = str(uuid4())
    posted_by_user_id = str(uuid4())
    user_id = str(uuid4())
    posted_at = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'feed/{user_id}',
        'schemaVersion': 2,
        'gsiA1PartitionKey': f'feed/{user_id}',
        'gsiA1SortKey': posted_at,
        'gsiA2PartitionKey': f'feed/{user_id}',
        'gsiA2SortKey': posted_by_user_id,
    }
    dynamo_table.put_item(Item=item)
    yield item


fi1 = feed_item
fi2 = feed_item
fi3 = feed_item


def test_nothing_to_migrate(dynamo_client, dynamo_table, dynamo_feed_table, caplog):
    # add something to the db to ensure it doesn't migrate
    key = {'partitionKey': 'not-a-feed-item', 'sortKey': '-'}
    dynamo_table.put_item(Item=key)
    assert dynamo_table.get_item(Key=key)['Item'] == key
    assert len(dynamo_table.scan()['Items']) == 1
    assert len(dynamo_feed_table.scan()['Items']) == 0

    # do the migration
    migration = Migration(dynamo_client, dynamo_table, dynamo_feed_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert len(dynamo_table.scan()['Items']) == 1
    assert dynamo_table.get_item(Key=key)['Item'] == key
    assert len(dynamo_feed_table.scan()['Items']) == 0


def test_migrate_one(dynamo_client, dynamo_table, dynamo_feed_table, caplog, feed_item):
    key = {k: feed_item[k] for k in ('partitionKey', 'sortKey')}
    post_id = key['partitionKey'].split('/')[1]
    feed_user_id = key['sortKey'].split('/')[1]
    new_key = {'postId': post_id, 'feedUserId': feed_user_id}
    assert len(dynamo_table.scan()['Items']) == 1
    assert dynamo_table.get_item(Key=key)['Item'] == feed_item
    assert len(dynamo_feed_table.scan()['Items']) == 0

    # do the migration
    migration = Migration(dynamo_client, dynamo_table, dynamo_feed_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in str(caplog.records[0])
    assert post_id in str(caplog.records[0])
    assert feed_user_id in str(caplog.records[0])

    # verify final state
    assert len(dynamo_table.scan()['Items']) == 1
    assert dynamo_table.get_item(Key=key)['Item'] == feed_item
    assert len(dynamo_feed_table.scan()['Items']) == 1
    assert dynamo_feed_table.get_item(Key=new_key)['Item'] == {
        'postId': post_id,
        'postedAt': feed_item['gsiA1SortKey'],
        'postedByUserId': feed_item['gsiA2SortKey'],
        'feedUserId': feed_user_id,
    }


def test_migrate_multiple(dynamo_client, dynamo_table, dynamo_feed_table, caplog, fi1, fi2, fi3):
    old_items = [fi1, fi2, fi3]
    old_keys = [{k: i[k] for k in ('partitionKey', 'sortKey')} for i in old_items]
    pairs = [(k['partitionKey'].split('/')[1], k['sortKey'].split('/')[1]) for k in old_keys]
    new_keys = [{'postId': p[0], 'feedUserId': p[1]} for p in pairs]
    assert len(dynamo_table.scan()['Items']) == 3
    for key, item in zip(old_keys, old_items):
        assert dynamo_table.get_item(Key=key)['Item'] == item
    assert len(dynamo_feed_table.scan()['Items']) == 0
    for key in new_keys:
        assert 'Item' not in dynamo_feed_table.get_item(Key=key)

    # do the migration, check logging
    migration = Migration(dynamo_client, dynamo_table, dynamo_feed_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert all('Migrating' in str(rec) for rec in caplog.records)
    for post_id, feed_user_id in pairs:
        assert sum(1 for rec in caplog.records if post_id in str(rec) and feed_user_id in str(rec)) == 1

    # check final state
    assert len(dynamo_table.scan()['Items']) == 3
    for key, item in zip(old_keys, old_items):
        assert dynamo_table.get_item(Key=key)['Item'] == item
    assert len(dynamo_feed_table.scan()['Items']) == 3
    for new_key, old_item, pair in zip(new_keys, old_items, pairs):
        new_item = dynamo_feed_table.get_item(Key=new_key)['Item']
        assert new_item == {
            'postId': pair[0],
            'postedByUserId': old_item['gsiA2SortKey'],
            'postedAt': old_item['gsiA1SortKey'],
            'feedUserId': pair[1],
        }

    # migrate again: will redo the migration but is idempotent
    caplog.clear()
    migration = Migration(dynamo_client, dynamo_table, dynamo_feed_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert all('Migrating' in str(rec) for rec in caplog.records)
    for post_id, feed_user_id in pairs:
        assert sum(1 for rec in caplog.records if post_id in str(rec) and feed_user_id in str(rec)) == 1
    assert len(dynamo_table.scan()['Items']) == 3
    assert len(dynamo_feed_table.scan()['Items']) == 3
