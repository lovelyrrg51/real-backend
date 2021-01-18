import logging
from uuid import uuid4

import pytest

from migrations.chat_0_0_fill_messages_count import Migration


@pytest.fixture
def already_migrated1(dynamo_table):
    chat_id = str(uuid4())
    item = {'partitionKey': f'chat/{chat_id}', 'sortKey': '-', 'chatId': chat_id}
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def already_migrated2(dynamo_table):
    chat_id = str(uuid4())
    item = {'partitionKey': f'chat/{chat_id}', 'sortKey': '-', 'chatId': chat_id, 'messagesCount': 42}
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def zero_message_count(dynamo_table):
    chat_id = str(uuid4())
    item = {'partitionKey': f'chat/{chat_id}', 'sortKey': '-', 'chatId': chat_id, 'messageCount': 0}
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def nonzero_message_count(dynamo_table):
    chat_id = str(uuid4())
    item = {'partitionKey': f'chat/{chat_id}', 'sortKey': '-', 'chatId': chat_id, 'messageCount': 7}
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def both_counts(dynamo_table):
    chat_id = str(uuid4())
    item = {
        'partitionKey': f'chat/{chat_id}',
        'sortKey': '-',
        'chatId': chat_id,
        'messageCount': 7,
        'messagesCount': 8,
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_migrate_nothing_to_migrate(dynamo_client, dynamo_table, caplog, already_migrated1, already_migrated2):
    # verify starting state
    items = [already_migrated1, already_migrated2]
    keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in items]
    for key, item in zip(keys, items):
        assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify final state
    for key, item in zip(keys, items):
        assert dynamo_table.get_item(Key=key)['Item'] == item


@pytest.mark.parametrize(
    'item, expected_messages_count',
    [
        [pytest.lazy_fixture('zero_message_count'), 0],
        [pytest.lazy_fixture('nonzero_message_count'), 7],
        [pytest.lazy_fixture('both_counts'), 15],
    ],
)
def test_migrate_one(dynamo_client, dynamo_table, caplog, item, expected_messages_count):
    # verify starting state
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert item['chatId'] in caplog.records[0].msg
    assert f'`{item["messageCount"]}`' in caplog.records[0].msg

    # verify final state
    new_item = dynamo_table.get_item(Key=key)['Item']
    assert 'messageCount' not in new_item
    assert new_item['messagesCount'] == expected_messages_count
    item.pop('messageCount')
    item['messagesCount'] = new_item['messagesCount']
    assert item == new_item


def test_migrate_multiple(
    dynamo_client, dynamo_table, caplog, zero_message_count, nonzero_message_count, both_counts
):
    items = [zero_message_count, nonzero_message_count, both_counts]

    # verify starting state
    keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in items]
    for key, item in zip(keys, items):
        assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    for item in items:
        assert sum(item['chatId'] in rec.msg for rec in caplog.records) == 1

    # verify final state
    for key in keys:
        new_item = dynamo_table.get_item(Key=key)['Item']
        assert 'messageCount' not in new_item
        assert 'messagesCount' in new_item
