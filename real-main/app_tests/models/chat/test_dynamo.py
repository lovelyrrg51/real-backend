import logging
from uuid import uuid4

import pendulum
import pytest

from app.models.chat.dynamo import ChatDynamo


@pytest.fixture
def chat_dynamo(dynamo_client):
    yield ChatDynamo(dynamo_client)


def test_transact_add_group_chat_minimal(chat_dynamo):
    chat_id = 'cid'
    chat_type = 'GROUP'
    user_id = 'cuid'

    # add the chat to the DB
    before = pendulum.now('utc')
    transact = chat_dynamo.transact_add(chat_id, chat_type, user_id)
    after = pendulum.now('utc')
    chat_dynamo.client.transact_write_items([transact])

    # retrieve the chat and verify all good
    chat_item = chat_dynamo.get(chat_id)
    created_at = pendulum.parse(chat_item['createdAt'])
    assert before <= created_at
    assert after >= created_at
    assert chat_item == {
        'partitionKey': 'chat/cid',
        'sortKey': '-',
        'schemaVersion': 0,
        'chatId': 'cid',
        'chatType': 'GROUP',
        'createdAt': created_at.to_iso8601_string(),
        'createdByUserId': 'cuid',
        'userCount': 1,
    }

    # verify we can't add another chat with same id
    with pytest.raises(chat_dynamo.client.exceptions.TransactionCanceledException):
        chat_dynamo.client.transact_write_items([transact])


def test_transact_add_group_chat_maximal(chat_dynamo):
    chat_id = 'cid'
    chat_type = 'GROUP'
    user_id = 'cuid'
    name = 'group name'

    # add the chat to the DB
    now = pendulum.now('utc')
    transact = chat_dynamo.transact_add(chat_id, chat_type, user_id, name=name, now=now)
    chat_dynamo.client.transact_write_items([transact])

    # retrieve the chat and verify all good
    chat_item = chat_dynamo.get(chat_id)
    assert chat_item == {
        'partitionKey': 'chat/cid',
        'sortKey': '-',
        'schemaVersion': 0,
        'chatId': 'cid',
        'chatType': 'GROUP',
        'createdAt': now.to_iso8601_string(),
        'createdByUserId': 'cuid',
        'userCount': 1,
        'name': 'group name',
    }

    # verify we can't add another chat with same id
    with pytest.raises(chat_dynamo.client.exceptions.TransactionCanceledException):
        chat_dynamo.client.transact_write_items([transact])


def test_transact_add_direct_chat_maximal(chat_dynamo):
    chat_id = 'cid2'
    chat_type = 'DIRECT'
    creator_user_id = 'uidb'
    with_user_id = 'uida'
    name = 'cname'
    now = pendulum.now('utc')

    # add the chat to the DB
    transact = chat_dynamo.transact_add(chat_id, chat_type, creator_user_id, with_user_id, name=name, now=now)
    chat_dynamo.client.transact_write_items([transact])

    # retrieve the chat and verify all good
    chat_item = chat_dynamo.get(chat_id)
    assert chat_item == {
        'partitionKey': 'chat/cid2',
        'sortKey': '-',
        'schemaVersion': 0,
        'gsiA1PartitionKey': 'chat/uida/uidb',
        'gsiA1SortKey': '-',
        'chatId': 'cid2',
        'chatType': 'DIRECT',
        'name': 'cname',
        'userCount': 2,
        'createdAt': now.to_iso8601_string(),
        'createdByUserId': 'uidb',
    }


def test_transact_add_errors(chat_dynamo):
    with pytest.raises(AssertionError, match='require with_user_id'):
        chat_dynamo.transact_add('cid', 'DIRECT', 'uid')

    with pytest.raises(AssertionError, match='forbit with_user_id'):
        chat_dynamo.transact_add('cid', 'GROUP', 'uid', with_user_id='uid')


def test_update_name(chat_dynamo):
    chat_id = 'cid'
    chat_type = 'ctype'
    user_id = 'uid'

    # add the chat to the DB, verify it is in DB
    transact = chat_dynamo.transact_add(chat_id, chat_type, user_id)
    chat_dynamo.client.transact_write_items([transact])
    assert 'name' not in chat_dynamo.get(chat_id)

    # update the chat name to something
    chat_dynamo.update_name(chat_id, 'new name')
    assert chat_dynamo.get(chat_id)['name'] == 'new name'

    # delete the chat name
    chat_dynamo.update_name(chat_id, '')
    assert 'name' not in chat_dynamo.get(chat_id)


def test_delete(chat_dynamo):
    chat_id = 'cid'
    chat_type = 'ctype'
    user_id = 'uid'

    # add the chat to the DB, verify it is in DB
    transact = chat_dynamo.transact_add(chat_id, chat_type, user_id)
    chat_dynamo.client.transact_write_items([transact])
    assert chat_dynamo.get(chat_id)

    # delete it, verify it was removed from DB
    assert chat_dynamo.delete(chat_id)
    assert chat_dynamo.get(chat_id) is None


def test_increment_decrement_user_count(chat_dynamo):
    chat_id = 'cid'
    chat_type = 'ctype'
    user_id = 'uid'

    # add the chat to the DB, verify it is in DB
    transact = chat_dynamo.transact_add(chat_id, chat_type, user_id)
    chat_dynamo.client.transact_write_items([transact])
    assert chat_dynamo.get(chat_id)['userCount'] == 1

    # increment
    transacts = [chat_dynamo.transact_increment_user_count(chat_id)]
    chat_dynamo.client.transact_write_items(transacts)
    assert chat_dynamo.get(chat_id)['userCount'] == 2

    # decrement
    transacts = [chat_dynamo.transact_decrement_user_count(chat_id)]
    chat_dynamo.client.transact_write_items(transacts)
    assert chat_dynamo.get(chat_id)['userCount'] == 1

    # decrement
    transacts = [chat_dynamo.transact_decrement_user_count(chat_id)]
    chat_dynamo.client.transact_write_items(transacts)
    assert chat_dynamo.get(chat_id)['userCount'] == 0

    # verify can't go below zero
    transacts = [chat_dynamo.transact_decrement_user_count(chat_id)]
    with pytest.raises(chat_dynamo.client.exceptions.TransactionCanceledException):
        chat_dynamo.client.transact_write_items(transacts)


def test_update_last_message_activity_at(chat_dynamo, caplog):
    # add the chat to the DB, verify it is in DB
    chat_id = str(uuid4())
    transact = chat_dynamo.transact_add(chat_id, 'chat-type', str(uuid4()))
    chat_dynamo.client.transact_write_items([transact])
    assert 'lastMessageActivityAt' not in chat_dynamo.get(chat_id)

    # verify we can update from not set
    now = pendulum.now('utc')
    assert (
        pendulum.parse(chat_dynamo.update_last_message_activity_at(chat_id, now)['lastMessageActivityAt']) == now
    )
    assert pendulum.parse(chat_dynamo.get(chat_id)['lastMessageActivityAt']) == now

    # verify we can update from set
    now = pendulum.now('utc')
    assert (
        pendulum.parse(chat_dynamo.update_last_message_activity_at(chat_id, now)['lastMessageActivityAt']) == now
    )
    assert pendulum.parse(chat_dynamo.get(chat_id)['lastMessageActivityAt']) == now

    # verify we fail soft
    before = now.subtract(seconds=10)
    with caplog.at_level(logging.WARNING):
        resp = chat_dynamo.update_last_message_activity_at(chat_id, before)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(
        x in caplog.records[0].msg
        for x in ['Failed', 'last message activity', chat_id, before.to_iso8601_string()]
    )
    assert resp is None
    assert pendulum.parse(chat_dynamo.get(chat_id)['lastMessageActivityAt']) == now


@pytest.mark.parametrize(
    'incrementor_name, decrementor_name, attribute_name',
    [
        ['increment_flag_count', 'decrement_flag_count', 'flagCount'],
        ['increment_messages_count', 'decrement_messages_count', 'messagesCount'],
    ],
)
def test_increment_decrement_count(chat_dynamo, caplog, incrementor_name, decrementor_name, attribute_name):
    incrementor = getattr(chat_dynamo, incrementor_name)
    decrementor = getattr(chat_dynamo, decrementor_name) if decrementor_name else None
    chat_id = str(uuid4())

    # can't increment message that doesnt exist
    with caplog.at_level(logging.WARNING):
        assert incrementor(chat_id) is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(x in caplog.records[0].msg for x in ['Failed to increment', attribute_name, chat_id])
    caplog.clear()

    # can't decrement message that doesnt exist
    if decrementor:
        with caplog.at_level(logging.WARNING):
            assert decrementor(chat_id) is None
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == 'WARNING'
        assert all(x in caplog.records[0].msg for x in ['Failed to decrement', attribute_name, chat_id])
        caplog.clear()

    # add the user to the DB, verify it is in DB
    transact = chat_dynamo.transact_add(chat_id, 'chat-type', str(uuid4()))
    chat_dynamo.client.transact_write_items([transact])
    assert attribute_name not in chat_dynamo.get(chat_id)

    # increment twice, verify
    assert incrementor(chat_id)[attribute_name] == 1
    assert chat_dynamo.get(chat_id)[attribute_name] == 1
    assert incrementor(chat_id)[attribute_name] == 2
    assert chat_dynamo.get(chat_id)[attribute_name] == 2

    # all done if there's no decrementor method
    if not decrementor:
        return

    # decrement twice, verify
    assert decrementor(chat_id)[attribute_name] == 1
    assert chat_dynamo.get(chat_id)[attribute_name] == 1
    assert decrementor(chat_id)[attribute_name] == 0
    assert chat_dynamo.get(chat_id)[attribute_name] == 0

    # verify fail soft on trying to decrement below zero
    with caplog.at_level(logging.WARNING):
        resp = decrementor(chat_id)
    assert resp is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(x in caplog.records[0].msg for x in ['Failed to decrement', attribute_name, chat_id])
    assert chat_dynamo.get(chat_id)[attribute_name] == 0
