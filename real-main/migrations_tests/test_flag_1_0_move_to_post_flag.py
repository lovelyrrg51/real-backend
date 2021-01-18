import logging
import uuid

import pendulum
import pytest

from migrations.flag_1_0_move_to_post_flag import Migration


@pytest.fixture
def flag(dynamo_table):
    post_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    now_str = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': f'flag/{user_id}/{post_id}',
        'sortKey': '-',
        'schemaVersion': 1,
        'postId': post_id,
        'flaggerUserId': user_id,
        'flaggedAt': now_str,
        'gsiA1PartitionKey': f'flag/{user_id}',
        'gsiA1SortKey': now_str,
        'gsiA2PartitionKey': f'flag/{post_id}',
        'gsiA2SortKey': now_str,
    }
    dynamo_table.put_item(Item=item)
    yield item


flag2 = flag
flag3 = flag


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog):
    # add something to the db to ensure it doesn't migrate
    pk = {'partitionKey': 'not-a-flag', 'sortKey': '-'}
    dynamo_table.put_item(Item=pk)
    assert dynamo_table.get_item(Key=pk)['Item'] == pk

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert dynamo_table.get_item(Key=pk)['Item'] == pk


def test_migrate_one(dynamo_client, dynamo_table, caplog, flag):
    post_id = flag['postId']
    user_id = flag['flaggerUserId']
    at_str = flag['flaggedAt']

    flag_pk = {'partitionKey': f'flag/{user_id}/{post_id}', 'sortKey': '-'}
    post_flag_pk = {'partitionKey': f'post/{post_id}', 'sortKey': f'flag/{user_id}'}

    # verify starting state
    assert dynamo_table.get_item(Key=flag_pk)['Item'] is not None
    assert 'Item' not in dynamo_table.get_item(Key=post_flag_pk)

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert post_id in str(caplog.records[0])
    assert user_id in str(caplog.records[0])

    # verify final state
    assert 'Item' not in dynamo_table.get_item(Key=flag_pk)
    assert dynamo_table.get_item(Key=post_flag_pk)['Item'] == {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'flag/{user_id}',
        'schemaVersion': 0,
        'createdAt': at_str,
        'gsiK1PartitionKey': f'flag/{user_id}',
        'gsiK1SortKey': '-',
    }


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, flag, flag2, flag3):
    flag_scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'flag/'},
    }
    post_flag_scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sk_prefix': 'flag/'},
    }

    # check starting state
    assert len(dynamo_table.scan(**flag_scan_kwargs)['Items']) == 3
    assert len(dynamo_table.scan(**post_flag_scan_kwargs)['Items']) == 0

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3

    # final starting state
    assert len(dynamo_table.scan(**flag_scan_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**post_flag_scan_kwargs)['Items']) == 3
