import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.feed_3_1_remove_from_main_table import Migration


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


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog):
    # add something to the db to ensure it doesn't migrate
    key = {'partitionKey': 'not-a-feed-item', 'sortKey': '-'}
    dynamo_table.put_item(Item=key)
    assert dynamo_table.get_item(Key=key)['Item'] == key
    assert len(dynamo_table.scan()['Items']) == 1

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert len(dynamo_table.scan()['Items']) == 1
    assert dynamo_table.get_item(Key=key)['Item'] == key


def test_migrate_one(dynamo_client, dynamo_table, caplog, feed_item):
    key = {k: feed_item[k] for k in ('partitionKey', 'sortKey')}
    post_id = key['partitionKey'].split('/')[1]
    feed_user_id = key['sortKey'].split('/')[1]
    assert len(dynamo_table.scan()['Items']) == 1
    assert dynamo_table.get_item(Key=key)['Item'] == feed_item

    # do the migration, verify final state
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Deleting' in caplog.records[0].msg
    assert post_id in caplog.records[0].msg
    assert feed_user_id in caplog.records[0].msg
    assert len(dynamo_table.scan()['Items']) == 0
    assert 'Item' not in dynamo_table.get_item(Key=key)


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, fi1, fi2, fi3):
    old_items = [fi1, fi2, fi3]
    old_keys = [{k: i[k] for k in ('partitionKey', 'sortKey')} for i in old_items]
    pairs = [(k['partitionKey'].split('/')[1], k['sortKey'].split('/')[1]) for k in old_keys]
    assert len(dynamo_table.scan()['Items']) == 3
    for key, item in zip(old_keys, old_items):
        assert dynamo_table.get_item(Key=key)['Item'] == item

    # do the migration, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert all('Deleting' in rec.msg for rec in caplog.records)
    for post_id, feed_user_id in pairs:
        assert sum(1 for rec in caplog.records if post_id in rec.msg and feed_user_id in rec.msg) == 1

    # check final state
    assert len(dynamo_table.scan()['Items']) == 0
    for key in old_keys:
        assert 'Item' not in dynamo_table.get_item(Key=key)

    # migrate again: no-op
    caplog.clear()
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert caplog.records == []
    assert len(dynamo_table.scan()['Items']) == 0
