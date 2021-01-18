import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.followedFirstStory_1_0_move_to_user_follower_first_story import Migration


@pytest.fixture
def distraction_item(dynamo_table):
    item = {
        'partitionKey': f'user/{uuid4()}',
        'sortKey': '-',
        'schemaVersion': 1,
        'someAttribute': 'lore ipsum',
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def ffs_deets(dynamo_table):
    followed_user_id, follower_user_id, post_id = str(uuid4()), str(uuid4()), str(uuid4())
    posted_at = pendulum.now('utc')
    expires_at = posted_at + pendulum.duration(days=1)
    item = {
        'partitionKey': f'followedFirstStory/{follower_user_id}/{followed_user_id}',
        'sortKey': '-',
        'schemaVersion': 1,
        'postId': post_id,
        'postedAt': posted_at.to_iso8601_string(),
        'postedByUserId': followed_user_id,
        'expiresAt': expires_at.to_iso8601_string(),
        'gsiA1PartitionKey': f'followedFirstStory/{follower_user_id}',
        'gsiA1SortKey': expires_at.to_iso8601_string(),
    }
    dynamo_table.put_item(Item=item)
    yield [item, followed_user_id, follower_user_id, post_id, expires_at]


ffs_deets1 = ffs_deets
ffs_deets2 = ffs_deets
ffs_deets3 = ffs_deets


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


def test_migrate_one(dynamo_client, dynamo_table, caplog, ffs_deets):
    old_item, followed_user_id, follower_user_id, post_id, expires_at = ffs_deets
    old_key = {k: old_item[k] for k in ('partitionKey', 'sortKey')}
    new_key = {
        'partitionKey': f'user/{followed_user_id}',
        'sortKey': f'follower/{follower_user_id}/firstStory',
    }
    new_item = {
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

    # verify starting state
    assert dynamo_table.get_item(Key=old_key)['Item'] == old_item
    assert 'Item' not in dynamo_table.get_item(Key=new_key)

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in str(caplog.records[0])
    assert followed_user_id in str(caplog.records[0])
    assert follower_user_id in str(caplog.records[0])

    # verify final state
    assert 'Item' not in dynamo_table.get_item(Key=old_key)
    assert dynamo_table.get_item(Key=new_key)['Item'] == new_item


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, ffs_deets1, ffs_deets2, ffs_deets3):
    _, followed_uid1, follower_uid1, _, _ = ffs_deets1
    _, followed_uid2, follower_uid2, _, _ = ffs_deets2
    _, followed_uid3, follower_uid3, _, _ = ffs_deets3
    old_key1 = {'partitionKey': f'followedFirstStory/{follower_uid1}/{followed_uid1}', 'sortKey': '-'}
    old_key2 = {'partitionKey': f'followedFirstStory/{follower_uid2}/{followed_uid2}', 'sortKey': '-'}
    old_key3 = {'partitionKey': f'followedFirstStory/{follower_uid3}/{followed_uid3}', 'sortKey': '-'}
    new_key1 = {'partitionKey': f'user/{followed_uid1}', 'sortKey': f'follower/{follower_uid1}/firstStory'}
    new_key2 = {'partitionKey': f'user/{followed_uid2}', 'sortKey': f'follower/{follower_uid2}/firstStory'}
    new_key3 = {'partitionKey': f'user/{followed_uid3}', 'sortKey': f'follower/{follower_uid3}/firstStory'}

    # verify starting state
    assert dynamo_table.get_item(Key=old_key1)['Item']
    assert dynamo_table.get_item(Key=old_key2)['Item']
    assert dynamo_table.get_item(Key=old_key3)['Item']
    assert 'Item' not in dynamo_table.get_item(Key=new_key1)
    assert 'Item' not in dynamo_table.get_item(Key=new_key2)
    assert 'Item' not in dynamo_table.get_item(Key=new_key3)

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert all('Migrating' in str(rec) for rec in caplog.records)
    assert sum(followed_uid1 in str(rec) for rec in caplog.records) == 1
    assert sum(followed_uid2 in str(rec) for rec in caplog.records) == 1
    assert sum(followed_uid3 in str(rec) for rec in caplog.records) == 1
    assert sum(follower_uid1 in str(rec) for rec in caplog.records) == 1
    assert sum(follower_uid2 in str(rec) for rec in caplog.records) == 1
    assert sum(follower_uid3 in str(rec) for rec in caplog.records) == 1

    # verify final state
    assert 'Item' not in dynamo_table.get_item(Key=old_key1)
    assert 'Item' not in dynamo_table.get_item(Key=old_key2)
    assert 'Item' not in dynamo_table.get_item(Key=old_key3)
    assert dynamo_table.get_item(Key=new_key1)['Item']
    assert dynamo_table.get_item(Key=new_key2)['Item']
    assert dynamo_table.get_item(Key=new_key3)['Item']
