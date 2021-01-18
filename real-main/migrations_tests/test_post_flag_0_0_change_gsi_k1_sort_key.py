import logging
import uuid

import pendulum
import pytest

from migrations.post_flag_0_0_change_gsi_k1_sort_key import Migration


@pytest.fixture
def post_flag(dynamo_table):
    post_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'flag/{user_id}',
        'schemaVersion': 0,
        'createdAt': pendulum.now('utc').to_iso8601_string(),
        'gsiK1PartitionKey': f'flag/{user_id}',
        'gsiK1SortKey': '-',
    }
    dynamo_table.put_item(Item=item)
    yield item


post_flag_1 = post_flag
post_flag_2 = post_flag
post_flag_3 = post_flag


@pytest.fixture
def post_flag_already_migrated(dynamo_table):
    post_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'flag/{user_id}',
        'schemaVersion': 0,
        'createdAt': pendulum.now('utc').to_iso8601_string(),
        'gsiK1PartitionKey': f'flag/{user_id}',
        'gsiK1SortKey': 'post',
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_migrate_no_items(dynamo_table, caplog, post_flag_already_migrated):
    pk = {k: post_flag_already_migrated[k] for k in ('partitionKey', 'sortKey')}

    # do the migration, verify no logs
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify no changes
    assert dynamo_table.get_item(Key=pk)['Item'] == post_flag_already_migrated


def test_migrate_one_item(dynamo_table, caplog, post_flag):
    pk = {k: post_flag[k] for k in ('partitionKey', 'sortKey')}

    # do the migration, verify logs
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert pk['partitionKey'] in str(caplog.records[0])
    assert pk['sortKey'] in str(caplog.records[0])
    assert 'migrating' in str(caplog.records[0])

    # verify correct changes to item
    item = dynamo_table.get_item(Key=pk)['Item']
    assert item.pop('gsiK1SortKey') == 'post'
    post_flag.pop('gsiK1SortKey')
    assert item == post_flag


def test_migrate_multiple_item(
    dynamo_table, caplog, post_flag_1, post_flag_2, post_flag_3, post_flag_already_migrated
):
    pk_1 = {k: post_flag_1[k] for k in ('partitionKey', 'sortKey')}
    pk_2 = {k: post_flag_2[k] for k in ('partitionKey', 'sortKey')}
    pk_3 = {k: post_flag_3[k] for k in ('partitionKey', 'sortKey')}
    pk_already = {k: post_flag_already_migrated[k] for k in ('partitionKey', 'sortKey')}

    # do the migration, verify logs
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    for record in caplog.records:
        assert 'migrating' in str(record)
    assert pk_1['partitionKey'] in str(caplog.records[0])
    assert pk_2['partitionKey'] in str(caplog.records[1])
    assert pk_3['partitionKey'] in str(caplog.records[2])

    # verify correct items changed
    assert dynamo_table.get_item(Key=pk_1)['Item'] != post_flag_1
    assert dynamo_table.get_item(Key=pk_2)['Item'] != post_flag_2
    assert dynamo_table.get_item(Key=pk_3)['Item'] != post_flag_3
    assert dynamo_table.get_item(Key=pk_already)['Item'] == post_flag_already_migrated

    # do the migration again, verify no logs
    caplog.clear()
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
