import logging
from decimal import Decimal
from uuid import uuid4

import pendulum
import pytest

from migrations.trending_0_3_remove_gsi_k3 import Migration

PERCISION = Decimal(10) ** -9


@pytest.fixture
def post_trending(dynamo_table):
    item_id = str(uuid4())
    item_type = 'post'
    initial_score = Decimal(1 / 6)
    now_str = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': f'{item_type}/{item_id}',
        'sortKey': 'trending',
        'schemaVersion': 0,
        'gsiA4PartitionKey': f'{item_type}/trending',
        'gsiA4SortKey': initial_score.quantize(PERCISION).normalize(),
        'gsiK3PartitionKey': f'{item_type}/trending',
        'gsiK3SortKey': initial_score.quantize(PERCISION).normalize(),
        'lastDeflatedAt': now_str,
        'createdAt': now_str,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_trending(dynamo_table):
    item_id = str(uuid4())
    item_type = 'user'
    initial_score = Decimal(5)
    now_str = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': f'{item_type}/{item_id}',
        'sortKey': 'trending',
        'schemaVersion': 0,
        'gsiA4PartitionKey': f'{item_type}/trending',
        'gsiA4SortKey': initial_score.quantize(PERCISION).normalize(),
        'gsiK3PartitionKey': f'{item_type}/trending',
        'gsiK3SortKey': initial_score.quantize(PERCISION).normalize(),
        'lastDeflatedAt': now_str,
        'createdAt': now_str,
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog):
    # add something to the db to ensure it doesn't migrate
    pk = {'partitionKey': 'unrelated-item', 'sortKey': '-'}
    dynamo_table.put_item(Item=pk)
    assert dynamo_table.get_item(Key=pk)['Item'] == pk

    # do the migration, check unrelated item was not affected
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert dynamo_table.get_item(Key=pk)['Item'] == pk


@pytest.mark.parametrize('item', pytest.lazy_fixture(['post_trending', 'user_trending']))
def test_migrate_one(dynamo_client, dynamo_table, caplog, item):
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == item
    assert 'gsiK3PartitionKey' in item
    assert 'gsiK3SortKey' in item

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in str(caplog.records[0])
    assert item['partitionKey'] in str(caplog.records[0])
    assert item['sortKey'] in str(caplog.records[0])

    # verify final state
    assert item.pop('gsiK3PartitionKey')
    assert item.pop('gsiK3SortKey')
    assert dynamo_table.get_item(Key=key)['Item'] == item


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, post_trending, user_trending):
    # check starting state
    scan_kwargs = {
        'FilterExpression': 'sortKey = :sk',
        'ExpressionAttributeValues': {':sk': 'trending'},
    }
    items = list(dynamo_table.scan(**scan_kwargs)['Items'])
    assert len(items) == 2
    assert sum(1 for item in items if 'gsiK3PartitionKey' in item) == 2
    assert sum(1 for item in items if 'gsiK3SortKey' in item) == 2

    # do the migration, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 2
    assert all('Migrating' in str(rec) for rec in caplog.records)
    assert sum(1 for rec in caplog.records if post_trending['partitionKey'] in str(rec)) == 1
    assert sum(1 for rec in caplog.records if user_trending['partitionKey'] in str(rec)) == 1

    # check state
    items = list(dynamo_table.scan(**scan_kwargs)['Items'])
    assert len(items) == 2
    assert sum(1 for item in items if 'gsiK3PartitionKey' in item) == 0
    assert sum(1 for item in items if 'gsiK3SortKey' in item) == 0

    # migrate again, check logging implies no-op
    caplog.clear()
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
