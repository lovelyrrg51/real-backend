import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.chat_message_view_0_delete import Migration


@pytest.fixture
def distractions(dynamo_table):
    message_id = str(uuid4())
    user_id = str(uuid4())
    items = [
        {'partitionKey': f'chatMessage/{message_id}', 'sortKey': f'flag/{user_id}', 'm': 2},
        {'partitionKey': f'chat/{message_id}', 'sortKey': f'view/{user_id}', 'n': 4},
    ]
    for item in items:
        dynamo_table.put_item(Item=item)
    yield items


@pytest.fixture
def chat_message_view1(dynamo_table):
    message_id = str(uuid4())
    user_id = str(uuid4())
    now_str = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': f'chatMessage/{message_id}',
        'sortKey': f'view/{user_id}',
        'schemaVersion': 0,
        'firstViewedAt': now_str,
        'lastViewedAt': now_str,
        'viewCount': 1,
        'gsiK1PartitionKey': f'chatMessage/{message_id}',
        'gsiK1SortKey': f'view/{now_str}',
    }
    dynamo_table.put_item(Item=item)
    yield item


chat_message_view2 = chat_message_view1
chat_message_view3 = chat_message_view1


@pytest.fixture
def chat_message_views(chat_message_view1, chat_message_view2, chat_message_view3):
    yield [chat_message_view1, chat_message_view2, chat_message_view3]


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog, distractions):
    # verify starting state
    keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in distractions]
    for key, item in zip(keys, distractions):
        assert dynamo_table.get_item(Key=key)['Item'] == item

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify state has not changed
    for key, item in zip(keys, distractions):
        assert dynamo_table.get_item(Key=key)['Item'] == item


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, chat_message_views, distractions):
    dist_keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in distractions]
    cmv_keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in chat_message_views]

    # check starting state
    for key, item in zip(dist_keys, distractions):
        assert dynamo_table.get_item(Key=key)['Item'] == item
    for key, item in zip(cmv_keys, chat_message_views):
        assert dynamo_table.get_item(Key=key)['Item'] == item

    # do the migration, check state
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert sum('Deleting' in rec.msg for rec in caplog.records) == 3
    for cmv in chat_message_views:
        assert sum(cmv['partitionKey'] in rec.msg for rec in caplog.records) == 1
        assert sum(cmv['sortKey'] in rec.msg for rec in caplog.records) == 1

    # check final state
    for key, item in zip(dist_keys, distractions):
        assert dynamo_table.get_item(Key=key)['Item'] == item
    for key in cmv_keys:
        assert 'Item' not in dynamo_table.get_item(Key=key)
