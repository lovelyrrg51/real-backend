import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.user_follower_first_story_1_0_drop_extra_fields import Migration


@pytest.fixture
def distraction_item(dynamo_table):
    item = {
        'partitionKey': f'user/{uuid4()}',
        'sortKey': 'follower/{uuid4()}',
        'schemaVersion': 1,
        'someAttribute': 'lore ipsum',
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def item(dynamo_table):
    followed_user_id, follower_user_id, post_id = str(uuid4()), str(uuid4()), str(uuid4())
    posted_at = pendulum.now('utc')
    expires_at = posted_at + pendulum.duration(days=1)
    item = {
        'schemaVersion': 1,
        'partitionKey': f'user/{followed_user_id}',
        'sortKey': f'follower/{follower_user_id}/firstStory',
        'gsiA1PartitionKey': f'followedFirstStory/{follower_user_id}',
        'gsiA1SortKey': expires_at.to_iso8601_string(),
        'gsiA2PartitionKey': f'follower/{follower_user_id}/firstStory',
        'gsiA2SortKey': expires_at.to_iso8601_string(),
        'postedByUserId': followed_user_id,
        'postId': post_id,
    }
    dynamo_table.put_item(Item=item)
    yield item


item1 = item
item2 = item
item3 = item


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog, distraction_item):
    item = distraction_item
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}

    # check starting state
    assert dynamo_table.get_item(Key=key)['Item'] == item

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify no change in db
    assert dynamo_table.get_item(Key=key)['Item'] == item


def test_migrate_one(dynamo_client, dynamo_table, caplog, item):
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    attrs_removed = ['gsiA1PartitionKey', 'gsiA1SortKey', 'postedByUserId']
    new_item = {k: item[k] for k in item.keys() if k not in attrs_removed}

    # verify starting state
    assert dynamo_table.get_item(Key=key)['Item'] == item

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in str(caplog.records[0])
    assert str(key) in str(caplog.records[0])

    # verify final state
    assert dynamo_table.get_item(Key=key)['Item'] == new_item


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, item1, item2, item3):
    key1 = {k: item1[k] for k in ('partitionKey', 'sortKey')}
    key2 = {k: item2[k] for k in ('partitionKey', 'sortKey')}
    key3 = {k: item3[k] for k in ('partitionKey', 'sortKey')}
    attrs_removed = ['gsiA1PartitionKey', 'gsiA1SortKey', 'postedByUserId']
    new_item1 = {k: item1[k] for k in item1.keys() if k not in attrs_removed}
    new_item2 = {k: item2[k] for k in item2.keys() if k not in attrs_removed}
    new_item3 = {k: item3[k] for k in item3.keys() if k not in attrs_removed}

    # verify starting state
    assert dynamo_table.get_item(Key=key1)['Item'] == item1
    assert dynamo_table.get_item(Key=key2)['Item'] == item2
    assert dynamo_table.get_item(Key=key3)['Item'] == item3

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert all('Migrating' in str(rec) for rec in caplog.records)
    assert sum(str(key1) in str(rec) for rec in caplog.records) == 1
    assert sum(str(key2) in str(rec) for rec in caplog.records) == 1
    assert sum(str(key3) in str(rec) for rec in caplog.records) == 1

    # verify final state
    assert dynamo_table.get_item(Key=key1)['Item'] == new_item1
    assert dynamo_table.get_item(Key=key2)['Item'] == new_item2
    assert dynamo_table.get_item(Key=key3)['Item'] == new_item3

    # migrate again, verify no affect
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify final state
    assert dynamo_table.get_item(Key=key1)['Item'] == new_item1
    assert dynamo_table.get_item(Key=key2)['Item'] == new_item2
    assert dynamo_table.get_item(Key=key3)['Item'] == new_item3
