import logging
import uuid

import pytest

from migrations.user_9_to_10_fill_followers_requested_count import Migration


@pytest.fixture
def already_migrated(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'userId': user_id,
        'schemaVersion': 10,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_none_requested(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'userId': user_id,
        'schemaVersion': 9,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_two_requested(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'userId': user_id,
        'schemaVersion': 9,
    }
    dynamo_table.put_item(Item=item)
    dynamo_table.put_item(
        Item={
            'partitionKey': f'user/{user_id}',
            'sortKey': f'follower/{uuid.uuid4()}',
            'gsiA2PartitionKey': f'followed/{user_id}',
            'gsiA2SortKey': 'REQUESTED/iso8601-string',
        }
    )
    dynamo_table.put_item(
        Item={
            'partitionKey': f'user/{user_id}',
            'sortKey': f'follower/{uuid.uuid4()}',
            'gsiA2PartitionKey': f'followed/{user_id}',
            'gsiA2SortKey': 'REQUESTED/iso8601-string',
        }
    )
    yield item


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog, already_migrated):
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


def test_migrate_none_requested(dynamo_client, dynamo_table, caplog, user_none_requested):
    # verify starting state
    item = user_none_requested
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert item['userId'] in caplog.records[0].msg
    assert '`0`' in caplog.records[0].msg

    # verify final state
    new_item = dynamo_table.get_item(Key=key)['Item']
    assert new_item.pop('schemaVersion') == 10
    assert item.pop('schemaVersion') == 9
    assert new_item == item


def test_migrate_two_requested(dynamo_client, dynamo_table, caplog, user_two_requested):
    # verify starting state
    item = user_two_requested
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert item['userId'] in caplog.records[0].msg
    assert '`2`' in caplog.records[0].msg

    # verify final state
    new_item = dynamo_table.get_item(Key=key)['Item']
    assert new_item.pop('followersRequestedCount') == 2
    assert new_item.pop('schemaVersion') == 10
    assert item.pop('schemaVersion') == 9
    assert new_item == item


def test_migrate_two_requested_one_already_counted(dynamo_client, dynamo_table, caplog, user_two_requested):
    item = user_two_requested
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}

    dynamo_table.update_item(
        Key=key,
        UpdateExpression='SET followersRequestedCount = :rfc',
        ConditionExpression='attribute_exists(partitionKey)',
        ExpressionAttributeValues={':rfc': 1},
    )
    item['followersRequestedCount'] = 1

    # verify starting state
    assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert item['userId'] in caplog.records[0].msg
    assert '`2`' in caplog.records[0].msg

    # verify final state
    new_item = dynamo_table.get_item(Key=key)['Item']
    assert new_item.pop('followersRequestedCount') == 2
    assert new_item.pop('schemaVersion') == 10
    assert item.pop('followersRequestedCount') == 1
    assert item.pop('schemaVersion') == 9
    assert new_item == item


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, user_none_requested, user_two_requested):
    items = [user_none_requested, user_two_requested]

    # verify starting state
    keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in items]
    for key, item in zip(keys, items):
        assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 2
    for item in items:
        assert sum(item['userId'] in rec.msg for rec in caplog.records) == 1

    # verify final state
    for key, item in zip(keys, items):
        new_item = dynamo_table.get_item(Key=key)['Item']
        new_item.pop('followersRequestedCount', None)  # varies by item
        item.pop('followersRequestedCount', None)  # varies by item
        assert new_item.pop('schemaVersion') == 10
        assert item.pop('schemaVersion') == 9
        assert new_item == item


def test_migrate_two_requested_race_condition(dynamo_client, dynamo_table, caplog, user_two_requested):
    item = user_two_requested
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}

    dynamo_table.update_item(
        Key=key,
        UpdateExpression='SET followersRequestedCount = :rfc',
        ConditionExpression='attribute_exists(partitionKey)',
        ExpressionAttributeValues={':rfc': 1},
    )
    updated_item = dynamo_table.get_item(Key=key)['Item']

    # verify starting state
    assert dynamo_table.get_item(Key=key)['Item'] != item
    assert dynamo_table.get_item(Key=key)['Item'] == updated_item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with pytest.raises(dynamo_client.exceptions.ConditionalCheckFailedException):
        migration.set_followers_requested_count(item, 2)
    assert len(caplog.records) == 1
    assert item['userId'] in caplog.records[0].msg
    assert '`2`' in caplog.records[0].msg

    # verify final state hasn't changed
    assert dynamo_table.get_item(Key=key)['Item'] == updated_item


def test_migrate_two_requested_race_condition_2(dynamo_client, dynamo_table, caplog, user_two_requested):
    item = user_two_requested
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}

    dynamo_table.update_item(
        Key=key,
        UpdateExpression='SET followersRequestedCount = :rfc',
        ConditionExpression='attribute_exists(partitionKey)',
        ExpressionAttributeValues={':rfc': 1},
    )
    item = dynamo_table.get_item(Key=key)['Item']

    dynamo_table.update_item(
        Key=key,
        UpdateExpression='REMOVE followersRequestedCount',
        ConditionExpression='attribute_exists(partitionKey)',
    )
    updated_item = dynamo_table.get_item(Key=key)['Item']

    # verify starting state
    assert dynamo_table.get_item(Key=key)['Item'] != item
    assert dynamo_table.get_item(Key=key)['Item'] == updated_item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with pytest.raises(dynamo_client.exceptions.ConditionalCheckFailedException):
        migration.set_followers_requested_count(item, 2)
    assert len(caplog.records) == 1
    assert item['userId'] in caplog.records[0].msg
    assert '`2`' in caplog.records[0].msg

    # verify final state hasn't changed
    assert dynamo_table.get_item(Key=key)['Item'] == updated_item
