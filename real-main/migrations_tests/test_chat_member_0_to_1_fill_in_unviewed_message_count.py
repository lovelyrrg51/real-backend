import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.chat_member_0_to_1_fill_in_unviewed_message_count import Migration


@pytest.fixture
def already_migrated(dynamo_table):
    chat_id, user_id = str(uuid4()), str(uuid4())
    dynamo_table.put_item(
        Item={
            'partitionKey': f'chat/{chat_id}',
            'sortKey': f'member/{user_id}',
            'schemaVersion': 1,
            'chatId': chat_id,
            'userId': user_id,
        }
    )
    yield dynamo_table.get_item(Key={'partitionKey': f'chat/{chat_id}', 'sortKey': f'member/{user_id}'})['Item']


@pytest.fixture
def no_messages_at_all(dynamo_table):
    chat_id, user_id = str(uuid4()), str(uuid4())
    dynamo_table.put_item(
        Item={
            'partitionKey': f'chat/{chat_id}',
            'sortKey': f'member/{user_id}',
            'schemaVersion': 0,
            'chatId': chat_id,
            'userId': user_id,
        }
    )
    yield dynamo_table.get_item(Key={'partitionKey': f'chat/{chat_id}', 'sortKey': f'member/{user_id}'})['Item']


@pytest.fixture
def one_message_author(dynamo_table):
    chat_id, user_id, message_id = str(uuid4()), str(uuid4()), str(uuid4())
    dynamo_table.put_item(
        Item={
            'partitionKey': f'chatMessage/{message_id}',
            'sortKey': '-',
            'messageId': message_id,
            'userId': user_id,
            'chatId': chat_id,
            'gsiA1PartitionKey': f'chatMessage/{chat_id}',
            'gsiA1SortKey': pendulum.now('utc').to_iso8601_string(),
        }
    )
    dynamo_table.put_item(
        Item={
            'partitionKey': f'chat/{chat_id}',
            'sortKey': f'member/{user_id}',
            'schemaVersion': 0,
            'chatId': chat_id,
            'userId': user_id,
        }
    )
    yield dynamo_table.get_item(Key={'partitionKey': f'chat/{chat_id}', 'sortKey': f'member/{user_id}'})['Item']


@pytest.fixture
def one_message_viewed(dynamo_table):
    chat_id, user_id, message_id = str(uuid4()), str(uuid4()), str(uuid4())
    dynamo_table.put_item(
        Item={
            'partitionKey': f'chatMessage/{message_id}',
            'sortKey': '-',
            'messageId': message_id,
            'userId': str(uuid4()),
            'chatId': chat_id,
            'gsiA1PartitionKey': f'chatMessage/{chat_id}',
            'gsiA1SortKey': pendulum.now('utc').to_iso8601_string(),
        }
    )
    dynamo_table.put_item(Item={'partitionKey': f'chatMessage/{message_id}', 'sortKey': f'view/{user_id}'})
    dynamo_table.put_item(
        Item={
            'partitionKey': f'chat/{chat_id}',
            'sortKey': f'member/{user_id}',
            'schemaVersion': 0,
            'chatId': chat_id,
            'userId': user_id,
        }
    )
    yield dynamo_table.get_item(Key={'partitionKey': f'chat/{chat_id}', 'sortKey': f'member/{user_id}'})['Item']


@pytest.fixture
def one_message_unviewed_unrecorded(dynamo_table):
    chat_id, user_id, message_id = str(uuid4()), str(uuid4()), str(uuid4())
    dynamo_table.put_item(
        Item={
            'partitionKey': f'chatMessage/{message_id}',
            'sortKey': '-',
            'messageId': message_id,
            'userId': str(uuid4()),
            'chatId': chat_id,
            'gsiA1PartitionKey': f'chatMessage/{chat_id}',
            'gsiA1SortKey': pendulum.now('utc').to_iso8601_string(),
        }
    )
    dynamo_table.put_item(
        Item={
            'partitionKey': f'chat/{chat_id}',
            'sortKey': f'member/{user_id}',
            'schemaVersion': 0,
            'chatId': chat_id,
            'userId': user_id,
        }
    )
    yield dynamo_table.get_item(Key={'partitionKey': f'chat/{chat_id}', 'sortKey': f'member/{user_id}'})['Item']


@pytest.fixture
def one_message_unviewed_recorded(dynamo_table):
    chat_id, user_id, message_id = str(uuid4()), str(uuid4()), str(uuid4())
    dynamo_table.put_item(
        Item={
            'partitionKey': f'chatMessage/{message_id}',
            'sortKey': '-',
            'messageId': message_id,
            'userId': str(uuid4()),
            'chatId': chat_id,
            'gsiA1PartitionKey': f'chatMessage/{chat_id}',
            'gsiA1SortKey': pendulum.now('utc').to_iso8601_string(),
        }
    )
    dynamo_table.put_item(
        Item={
            'partitionKey': f'chat/{chat_id}',
            'sortKey': f'member/{user_id}',
            'schemaVersion': 0,
            'unviewedMessageCount': 1,
            'chatId': chat_id,
            'userId': user_id,
        }
    )
    yield dynamo_table.get_item(Key={'partitionKey': f'chat/{chat_id}', 'sortKey': f'member/{user_id}'})['Item']


@pytest.fixture
def multiple_messages(dynamo_table):
    "Four messages: one user is author, one user has viewed, and two user has not viewed"
    chat_id, user_id = str(uuid4()), str(uuid4())
    message_id1, message_id2, message_id3, message_id4 = str(uuid4()), str(uuid4()), str(uuid4()), str(uuid4())
    dynamo_table.put_item(  # message we have not viewed
        Item={
            'partitionKey': f'chatMessage/{message_id1}',
            'sortKey': '-',
            'messageId': message_id1,
            'userId': str(uuid4()),
            'chatId': chat_id,
            'gsiA1PartitionKey': f'chatMessage/{chat_id}',
            'gsiA1SortKey': pendulum.now('utc').to_iso8601_string(),
        }
    )
    dynamo_table.put_item(  # message we are author of
        Item={
            'partitionKey': f'chatMessage/{message_id2}',
            'sortKey': '-',
            'messageId': message_id2,
            'userId': user_id,
            'chatId': chat_id,
            'gsiA1PartitionKey': f'chatMessage/{chat_id}',
            'gsiA1SortKey': pendulum.now('utc').to_iso8601_string(),
        }
    )
    dynamo_table.put_item(  # message we have viewed
        Item={
            'partitionKey': f'chatMessage/{message_id3}',
            'sortKey': '-',
            'messageId': message_id3,
            'userId': str(uuid4()),
            'chatId': chat_id,
            'gsiA1PartitionKey': f'chatMessage/{chat_id}',
            'gsiA1SortKey': pendulum.now('utc').to_iso8601_string(),
        }
    )
    dynamo_table.put_item(Item={'partitionKey': f'chatMessage/{message_id3}', 'sortKey': f'view/{user_id}'})
    dynamo_table.put_item(  # message we have not viewed
        Item={
            'partitionKey': f'chatMessage/{message_id4}',
            'sortKey': '-',
            'messageId': message_id4,
            'userId': str(uuid4()),
            'chatId': chat_id,
            'gsiA1PartitionKey': f'chatMessage/{chat_id}',
            'gsiA1SortKey': pendulum.now('utc').to_iso8601_string(),
        }
    )
    dynamo_table.put_item(
        Item={
            'partitionKey': f'chat/{chat_id}',
            'sortKey': f'member/{user_id}',
            'schemaVersion': 0,
            'unviewedMessageCount': 1,  # starts with one of the two counted
            'chatId': chat_id,
            'userId': user_id,
        }
    )
    yield dynamo_table.get_item(Key={'partitionKey': f'chat/{chat_id}', 'sortKey': f'member/{user_id}'})['Item']


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


@pytest.mark.parametrize('item', pytest.lazy_fixture(['one_message_author', 'one_message_viewed']))
def test_migrate_one_viewed(dynamo_client, dynamo_table, caplog, item):
    # verify starting state
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert item['chatId'] in caplog.records[0].msg
    assert item['userId'] in caplog.records[0].msg
    assert '`0`' in caplog.records[0].msg

    # verify final state
    new_item = dynamo_table.get_item(Key=key)['Item']
    assert 'unviewedMessageCount' not in new_item
    assert new_item['schemaVersion'] == 1
    new_item['schemaVersion'] = item['schemaVersion']
    assert new_item == item


@pytest.mark.parametrize(
    'item', pytest.lazy_fixture(['one_message_unviewed_recorded', 'one_message_unviewed_unrecorded'])
)
def test_migrate_one_unviewed(dynamo_client, dynamo_table, caplog, item):
    # verify starting state
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert item['chatId'] in caplog.records[0].msg
    assert item['userId'] in caplog.records[0].msg
    assert '`1`' in caplog.records[0].msg

    # verify final state
    new_item = dynamo_table.get_item(Key=key)['Item']
    assert new_item.pop('unviewedMessageCount') == 1
    item.pop('unviewedMessageCount', None)  # no assert b/c diff for our diff fixture inputs
    assert new_item['schemaVersion'] == 1
    new_item['schemaVersion'] = item['schemaVersion']
    assert new_item == item


def test_migrate_multiple_messages(dynamo_client, dynamo_table, caplog, multiple_messages):
    item = multiple_messages

    # verify starting state
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert item['chatId'] in caplog.records[0].msg
    assert item['userId'] in caplog.records[0].msg
    assert '`2`' in caplog.records[0].msg

    # verify final state
    new_item = dynamo_table.get_item(Key=key)['Item']
    assert new_item.pop('unviewedMessageCount') == 2
    assert item.pop('unviewedMessageCount') == 1
    assert new_item['schemaVersion'] == 1
    new_item['schemaVersion'] = item['schemaVersion']
    assert new_item == item


def test_migrate_multiple(
    dynamo_client, dynamo_table, caplog, one_message_viewed, one_message_unviewed_unrecorded, multiple_messages
):
    items = [one_message_viewed, one_message_unviewed_unrecorded, multiple_messages]

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
        assert sum(item['userId'] in rec.msg for rec in caplog.records) == 1

    # verify final state
    for key, item in zip(keys, items):
        new_item = dynamo_table.get_item(Key=key)['Item']
        new_item.pop('unviewedMessageCount', None)  # varies by item
        item.pop('unviewedMessageCount', None)  # varies by item
        assert new_item['schemaVersion'] == 1
        new_item['schemaVersion'] = item['schemaVersion']
        assert new_item == item
