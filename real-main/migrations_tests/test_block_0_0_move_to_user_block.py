import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.block_0_0_move_to_user_block import Migration


@pytest.fixture
def block(dynamo_table):
    blocked_user_id = str(uuid4())
    blocker_user_id = str(uuid4())
    blocked_at_str = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': f'block/{blocker_user_id}/{blocked_user_id}',
        'sortKey': '-',
        'schemaVersion': 0,
        'gsiA1PartitionKey': f'block/{blocker_user_id}',
        'gsiA1SortKey': blocked_at_str,
        'gsiA2PartitionKey': f'block/{blocked_user_id}',
        'gsiA2SortKey': blocked_at_str,
        'blockerUserId': blocker_user_id,
        'blockedUserId': blocked_user_id,
        'blockedAt': blocked_at_str,
    }
    dynamo_table.put_item(Item=item)
    yield item


block2 = block
block3 = block


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog):
    # add something to the db to ensure it doesn't migrate
    pk = {'partitionKey': 'unrelated', 'sortKey': '-'}
    dynamo_table.put_item(Item=pk)
    assert dynamo_table.get_item(Key=pk)['Item'] == pk

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert dynamo_table.get_item(Key=pk)['Item'] == pk


def test_migrate_one(dynamo_client, dynamo_table, caplog, block):
    blocked_user_id = block['blockedUserId']
    blocker_user_id = block['blockerUserId']
    old_key = {'partitionKey': f'block/{blocker_user_id}/{blocked_user_id}', 'sortKey': '-'}
    new_key = {'partitionKey': f'user/{blocked_user_id}', 'sortKey': f'blocker/{blocker_user_id}'}

    # verify starting state
    old_item = dynamo_table.get_item(Key=old_key)['Item']
    assert old_item is not None
    assert 'Item' not in dynamo_table.get_item(Key=new_key)

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in str(caplog.records[0])
    assert blocked_user_id in str(caplog.records[0])
    assert blocker_user_id in str(caplog.records[0])

    # verify final state
    assert 'Item' not in dynamo_table.get_item(Key=old_key)
    new_item = dynamo_table.get_item(Key=new_key)['Item']
    assert new_item == {**old_item, **new_key}


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, block, block2, block3):
    old_scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'block/'},
    }
    new_scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'user/', ':sk_prefix': 'blocker/'},
    }

    # check starting state
    assert len(dynamo_table.scan(**old_scan_kwargs)['Items']) == 3
    assert len(dynamo_table.scan(**new_scan_kwargs)['Items']) == 0

    # do the migration, check state
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert all('Migrating' in str(rec) for rec in caplog.records)
    assert len(dynamo_table.scan(**old_scan_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**new_scan_kwargs)['Items']) == 3

    # do the migration again, check is no-op
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert len(dynamo_table.scan(**old_scan_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**new_scan_kwargs)['Items']) == 3
