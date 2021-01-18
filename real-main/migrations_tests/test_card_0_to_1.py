import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.card_0_to_1 import Migration


@pytest.fixture
def chat_card(dynamo_table):
    user_id = f'us-east-1:{uuid4()}'
    card_id = f'{user_id}:CHAT_ACTIVITY'
    created_at = pendulum.now('utc')
    item = {
        'partitionKey': f'card/{card_id}',
        'sortKey': '-',
        'schemaVersion': 0,
        'gsiA1PartitionKey': f'user/{user_id}',
        'gsiA1SortKey': f'card/{created_at.to_iso8601_string()}',
        'title': 'chat card title',
        'action': 'chat card action',
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def comment_card(dynamo_table):
    user_id = f'us-east-1:{uuid4()}'
    post_id = str(uuid4())
    card_id = f'{user_id}:COMMENT_ACTIVITY:{post_id}'
    created_at = pendulum.now('utc')
    item = {
        'partitionKey': f'card/{card_id}',
        'sortKey': '-',
        'schemaVersion': 0,
        'gsiA1PartitionKey': f'user/{user_id}',
        'gsiA1SortKey': f'card/{created_at.to_iso8601_string()}',
        'title': 'comment card title',
        'action': 'comment card action',
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post_likes_card(dynamo_table):
    user_id = f'us-east-1:{uuid4()}'
    post_id = str(uuid4())
    card_id = f'{user_id}:POST_LIKES:{post_id}'
    created_at = pendulum.now('utc')
    item = {
        'partitionKey': f'card/{card_id}',
        'sortKey': '-',
        'schemaVersion': 0,
        'gsiA1PartitionKey': f'user/{user_id}',
        'gsiA1SortKey': f'card/{created_at.to_iso8601_string()}',
        'title': 'post likes title',
        'action': 'post likes action',
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post_views_card(dynamo_table):
    user_id = f'us-east-1:{uuid4()}'
    post_id = str(uuid4())
    card_id = f'{user_id}:POST_VIEWS:{post_id}'
    created_at = pendulum.now('utc')
    item = {
        'partitionKey': f'card/{card_id}',
        'sortKey': '-',
        'schemaVersion': 0,
        'gsiA1PartitionKey': f'user/{user_id}',
        'gsiA1SortKey': f'card/{created_at.to_iso8601_string()}',
        'title': 'post views title',
        'action': 'post views action',
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def requested_followers_card(dynamo_table):
    user_id = f'us-east-1:{uuid4()}'
    card_id = f'{user_id}:REQUESTED_FOLLOWERS'
    created_at = pendulum.now('utc')
    item = {
        'partitionKey': f'card/{card_id}',
        'sortKey': '-',
        'schemaVersion': 0,
        'gsiA1PartitionKey': f'user/{user_id}',
        'gsiA1SortKey': f'card/{created_at.to_iso8601_string()}',
        'title': 'requested followers card title',
        'action': 'requested followers card action',
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog):
    # add something to the db to ensure it doesn't migrate
    pk = {'partitionKey': 'not-a-feed-item', 'sortKey': '-'}
    dynamo_table.put_item(Item=pk)
    assert dynamo_table.get_item(Key=pk)['Item'] == pk

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert dynamo_table.get_item(Key=pk)['Item'] == pk


@pytest.mark.parametrize('item', pytest.lazy_fixture(['chat_card', 'requested_followers_card']))
def test_migrate_one_no_post(dynamo_client, dynamo_table, caplog, item):
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == item
    card_id = item['partitionKey'].split('/')[1]

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in str(caplog.records[0])
    assert card_id in str(caplog.records[0])

    # verify final state
    new_item = dynamo_table.get_item(Key=key)['Item']
    assert new_item.pop('schemaVersion') == 1
    assert item.pop('schemaVersion') == 0
    assert new_item == item


@pytest.mark.parametrize('item', pytest.lazy_fixture(['comment_card', 'post_likes_card', 'post_views_card']))
def test_migrate_one_with_post(dynamo_client, dynamo_table, caplog, item):
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == item
    card_id = item['partitionKey'].split('/')[1]
    user_id = item['gsiA1PartitionKey'].split('/')[1]
    # extracting post_id in a different manner than is done in the migration
    post_id = card_id[::-1].split(':')[0][::-1]

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in str(caplog.records[0])
    assert card_id in str(caplog.records[0])

    # verify final state
    new_item = dynamo_table.get_item(Key=key)['Item']
    assert new_item.pop('schemaVersion') == 1
    assert new_item.pop('postId') == post_id
    assert new_item.pop('gsiA2PartitionKey').split('/') == ['card', post_id]
    assert new_item.pop('gsiA2SortKey') == user_id
    assert item.pop('schemaVersion') == 0
    assert new_item == item


def test_migrate_multiple(
    dynamo_client,
    dynamo_table,
    caplog,
    chat_card,
    requested_followers_card,
    comment_card,
    post_likes_card,
    post_views_card,
):
    # check starting state
    scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'card/'},
    }
    items = list(dynamo_table.scan(**scan_kwargs)['Items'])
    assert len(items) == 5
    assert sum(1 for item in items if item['partitionKey'].count(':') == 2) == 2
    assert sum(1 for item in items if item['partitionKey'].count(':') == 3) == 3
    assert sum(1 for item in items if item['schemaVersion'] == 0) == 5
    assert sum(1 for item in items if item['schemaVersion'] == 1) == 0
    assert sum(1 for item in items if 'postId' in item) == 0
    assert sum(1 for item in items if 'gsiA2PartitionKey' in item) == 0
    assert sum(1 for item in items if 'gsiA2SortKey' in item) == 0

    # do the migration, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 5
    for rec in caplog.records:
        assert 'Migrating' in str(rec)

    # check state
    items = list(dynamo_table.scan(**scan_kwargs)['Items'])
    assert len(items) == 5
    assert sum(1 for item in items if item['partitionKey'].count(':') == 2) == 2
    assert sum(1 for item in items if item['partitionKey'].count(':') == 3) == 3
    assert sum(1 for item in items if item['schemaVersion'] == 0) == 0
    assert sum(1 for item in items if item['schemaVersion'] == 1) == 5
    assert sum(1 for item in items if 'postId' in item) == 3
    assert sum(1 for item in items if 'gsiA2PartitionKey' in item) == 3
    assert sum(1 for item in items if 'gsiA2SortKey' in item) == 3

    # migrate again, check logging implies no-op
    caplog.clear()
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
