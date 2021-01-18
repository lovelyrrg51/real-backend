import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.following_1_0_move_to_user_follower import Migration


@pytest.fixture
def already_migrated(dynamo_table):
    follower_user_id, followed_user_id = str(uuid4()), str(uuid4())
    now_str = pendulum.now('utc').to_iso8601_string()
    follow_status = 'XXXXXXXXXXXX'
    key = {
        'partitionKey': f'user/{followed_user_id}',
        'sortKey': f'follower/{follower_user_id}',
    }
    dynamo_table.put_item(
        Item={
            **key,
            'schemaVersion': 1,
            'followedAt': now_str,
            'followStatus': follow_status,
            'followerUserId': follower_user_id,
            'followedUserId': followed_user_id,
            'gsiA1PartitionKey': f'follower/{follower_user_id}',
            'gsiA1SortKey': f'{follow_status}/{now_str}',
            'gsiA2PartitionKey': f'followed/{followed_user_id}',
            'gsiA2SortKey': f'{follow_status}/{now_str}',
        }
    )
    yield dynamo_table.get_item(Key=key)['Item']


@pytest.fixture
def following(dynamo_table):
    follower_user_id, followed_user_id = str(uuid4()), str(uuid4())
    now_str = pendulum.now('utc').to_iso8601_string()
    follow_status = 'YYYYYYYYYYY'
    key = {
        'partitionKey': f'following/{follower_user_id}/{followed_user_id}',
        'sortKey': '-',
    }
    dynamo_table.put_item(
        Item={
            **key,
            'schemaVersion': 1,
            'followedAt': now_str,
            'followStatus': follow_status,
            'followerUserId': follower_user_id,
            'followedUserId': followed_user_id,
            'gsiA1PartitionKey': f'follower/{follower_user_id}',
            'gsiA1SortKey': f'{follow_status}/{now_str}',
            'gsiA2PartitionKey': f'followed/{followed_user_id}',
            'gsiA2SortKey': f'{follow_status}/{now_str}',
        }
    )
    yield dynamo_table.get_item(Key=key)['Item']


following1 = following
following2 = following
following3 = following


def test_migrate_nothing_to_migrate(dynamo_client, dynamo_table, caplog, already_migrated):
    # verify starting state
    item = already_migrated
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify final state
    assert dynamo_table.get_item(Key=key)['Item'] == item


def test_migrate_one(dynamo_client, dynamo_table, caplog, following):
    item = following

    # verify starting state
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    new_key = {
        'partitionKey': 'user/' + item['followedUserId'],
        'sortKey': 'follower/' + item['followerUserId'],
    }
    assert dynamo_table.get_item(Key=key)['Item'] == item
    assert 'Item' not in dynamo_table.get_item(Key=new_key)

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert caplog.records[0].msg.count(item['followerUserId']) == 2
    assert caplog.records[0].msg.count(item['followedUserId']) == 2

    # verify final state
    assert 'Item' not in dynamo_table.get_item(Key=key)
    new_item = dynamo_table.get_item(Key=new_key)['Item']
    new_item['partitionKey'] = item['partitionKey']
    new_item['sortKey'] = item['sortKey']
    assert new_item == item


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, following1, following2, following3):
    items = [following1, following2, following3]

    # verify starting state
    keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in items]
    new_keys = [
        {'partitionKey': 'user/' + item['followedUserId'], 'sortKey': 'follower/' + item['followerUserId']}
        for item in items
    ]
    for key, new_key, item in zip(keys, new_keys, items):
        assert dynamo_table.get_item(Key=key)['Item'] == item
        assert 'Item' not in dynamo_table.get_item(Key=new_key)

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    for item in items:
        assert sum(item['followerUserId'] in rec.msg for rec in caplog.records) == 1
        assert sum(item['followedUserId'] in rec.msg for rec in caplog.records) == 1

    # verify final state
    for key, new_key, item in zip(keys, new_keys, items):
        assert 'Item' not in dynamo_table.get_item(Key=key)
        new_item = dynamo_table.get_item(Key=new_key)['Item']
        new_item['partitionKey'] = item['partitionKey']
        new_item['sortKey'] = item['sortKey']
        assert new_item == item


def test_status_condition_failure(dynamo_client, dynamo_table, following):
    item = following

    # verify starting state
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    new_key = {
        'partitionKey': 'user/' + item['followedUserId'],
        'sortKey': 'follower/' + item['followerUserId'],
    }
    assert dynamo_table.get_item(Key=key)['Item'] == item
    assert 'Item' not in dynamo_table.get_item(Key=new_key)

    typed_key = {k: {'S': key[k]} for k in ('partitionKey', 'sortKey')}
    typed_item = dynamo_client.get_item(Key=typed_key, TableName=dynamo_table.table_name).get('Item')

    # sneak behind our in-memory item's back and change the status in dynamo
    dynamo_table.update_item(
        Key=key,
        UpdateExpression='SET followStatus = :followStatus',
        ExpressionAttributeValues={':followStatus': 'new-status'},
    )
    updated_item = dynamo_table.get_item(Key=key)['Item']
    assert updated_item['followStatus'] == 'new-status'

    migration = Migration(dynamo_client, dynamo_table)

    with pytest.raises(dynamo_client.exceptions.TransactionCanceledException):
        migration.move_following(typed_item)

    # check state didn't change
    assert dynamo_table.get_item(Key=key)['Item'] == updated_item
    assert 'Item' not in dynamo_table.get_item(Key=new_key)
