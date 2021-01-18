import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.post_view_0_2_add_gsiA1_gsiA2 import Migration


@pytest.fixture
def post_view(dynamo_table):
    post_id = str(uuid4())
    user_id = str(uuid4())
    first_viewed_at_str = (pendulum.now('utc') - pendulum.duration(minutes=5)).to_iso8601_string()
    last_viewed_at_str = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'view/{user_id}',
        'gsiK1PartitionKey': f'post/{post_id}',
        'gsiK1SortKey': f'view/{first_viewed_at_str}',
        'schemaVersion': 0,
        'viewCount': 2,
        'firstViewedAt': first_viewed_at_str,
        'lastViewedAt': last_viewed_at_str,
    }
    dynamo_table.put_item(Item=item)
    yield item


pv1 = post_view
pv2 = post_view
pv3 = post_view


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog):
    # create a distration in the DB
    key = {'partitionKey': f'post/{uuid4()}', 'sortKey': '-'}
    dynamo_table.put_item(Item=key)
    assert dynamo_table.get_item(Key=key)['Item'] == key

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify final state
    assert dynamo_table.get_item(Key=key)['Item'] == key


def test_migrate_one(dynamo_client, dynamo_table, caplog, post_view):
    # verify starting state
    item = post_view
    post_id = item['partitionKey'].split('/')[1]
    user_id = item['sortKey'].split('/')[1]
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in caplog.records[0].msg
    assert post_id in caplog.records[0].msg
    assert user_id in caplog.records[0].msg

    # verify final state
    new_item = dynamo_table.get_item(Key=key)['Item']
    assert new_item.pop('gsiA1PartitionKey').split('/') == ['postView', post_id]
    assert new_item.pop('gsiA1SortKey') == item['firstViewedAt']
    assert new_item.pop('gsiA2PartitionKey').split('/') == ['postView', user_id]
    assert new_item.pop('gsiA2SortKey') == item['firstViewedAt']
    assert new_item == item


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, pv1, pv2, pv3):
    items = [pv1, pv2, pv3]

    # verify starting state
    keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in items]
    for key, item in zip(keys, items):
        assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3

    # verify final state
    for key, item in zip(keys, items):
        new_item = dynamo_table.get_item(Key=key)['Item']
        assert new_item.pop('gsiA1PartitionKey')
        assert new_item.pop('gsiA1SortKey')
        assert new_item.pop('gsiA2PartitionKey')
        assert new_item.pop('gsiA2SortKey')
        assert new_item == item

    # migrate again, test no-op
    caplog.clear()
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
