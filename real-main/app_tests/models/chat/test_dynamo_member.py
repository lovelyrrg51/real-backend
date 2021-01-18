import logging
from uuid import uuid4

import pendulum
import pytest

from app.models.chat.dynamo import ChatMemberDynamo


@pytest.fixture
def cm_dynamo(dynamo_client):
    yield ChatMemberDynamo(dynamo_client)


def test_transact_add(cm_dynamo):
    chat_id = 'cid2'
    user_id = 'uid'
    now = pendulum.now('utc')

    # add the chat membership to the DB
    transact = cm_dynamo.transact_add(chat_id, user_id, now=now)
    cm_dynamo.client.transact_write_items([transact])

    # retrieve the chat membership and verify all good
    item = cm_dynamo.get(chat_id, user_id)
    joined_at_str = now.to_iso8601_string()
    assert item == {
        'partitionKey': 'chat/cid2',
        'sortKey': 'member/uid',
        'schemaVersion': 1,
        'gsiK1PartitionKey': 'chat/cid2',
        'gsiK1SortKey': f'member/{joined_at_str}',
        'gsiK2PartitionKey': 'member/uid',
        'gsiK2SortKey': f'chat/{joined_at_str}',
    }


def test_delete(cm_dynamo):
    chat_id = 'cid'
    user_id = 'uid'

    # add the chat membership to the DB, verify it is in DB
    transact = cm_dynamo.transact_add(chat_id, user_id)
    cm_dynamo.client.transact_write_items([transact])
    assert cm_dynamo.get(chat_id, user_id)

    # delete it, verify it was removed from DB
    assert cm_dynamo.delete(chat_id, user_id)
    assert cm_dynamo.get(chat_id, user_id) is None


def test_update_last_message_activity_at(cm_dynamo, caplog):
    chat_id = 'cid'

    # add a member to the chat
    user_id = 'uid1'
    now = pendulum.now('utc')
    transact = cm_dynamo.transact_add(chat_id, user_id, now)
    cm_dynamo.client.transact_write_items([transact])

    # verify starting state
    item = cm_dynamo.get(chat_id, user_id)
    assert item['gsiK2SortKey'] == 'chat/' + now.to_iso8601_string()

    # update the last message activity at for that memeber
    new_now = pendulum.now('utc')
    new_item = cm_dynamo.update_last_message_activity_at(chat_id, user_id, new_now)
    assert new_item['gsiK2SortKey'] == 'chat/' + new_now.to_iso8601_string()

    # verify final state
    new_item = cm_dynamo.get(chat_id, user_id)
    assert new_item['gsiK2SortKey'] == 'chat/' + new_now.to_iso8601_string()
    item['gsiK2SortKey'] = new_item['gsiK2SortKey']
    assert item == new_item

    # verify we can fail soft on an update
    before = new_now.subtract(seconds=10)
    with caplog.at_level(logging.WARNING):
        resp = cm_dynamo.update_last_message_activity_at(chat_id, user_id, before)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(
        x in caplog.records[0].msg
        for x in ['Failed', 'last message activity', chat_id, user_id, before.to_iso8601_string()]
    )
    assert resp is None
    assert cm_dynamo.get(chat_id, user_id) == item


def test_generate_user_ids_by_chat(cm_dynamo):
    chat_id = 'cid'

    # verify nothing from non-existent chat / chat with no members
    assert list(cm_dynamo.generate_user_ids_by_chat(chat_id)) == []

    # add a member to the chat
    user_id_1 = 'uid1'
    transact = cm_dynamo.transact_add(chat_id, user_id_1)
    cm_dynamo.client.transact_write_items([transact])

    # verify we generate that user_id
    assert list(cm_dynamo.generate_user_ids_by_chat(chat_id)) == [user_id_1]

    # add another member to the chat
    user_id_2 = 'uid2'
    transact = cm_dynamo.transact_add(chat_id, user_id_2)
    cm_dynamo.client.transact_write_items([transact])

    # verify we generate both user_ids, in order
    assert list(cm_dynamo.generate_user_ids_by_chat(chat_id)) == [user_id_1, user_id_2]


def test_generate_chat_ids_by_chat(cm_dynamo):
    user_id = 'cid'

    # verify nothing
    assert list(cm_dynamo.generate_chat_ids_by_user(user_id)) == []

    # add user to a chat
    chat_id_1 = 'cid1'
    transact = cm_dynamo.transact_add(chat_id_1, user_id)
    cm_dynamo.client.transact_write_items([transact])

    # verify we generate that chat_id
    assert list(cm_dynamo.generate_chat_ids_by_user(user_id)) == [chat_id_1]

    # add user to another chat
    chat_id_2 = 'cid2'
    transact = cm_dynamo.transact_add(chat_id_2, user_id)
    cm_dynamo.client.transact_write_items([transact])

    # verify we generate both chat_ids, in order
    assert list(cm_dynamo.generate_chat_ids_by_user(user_id)) == [chat_id_1, chat_id_2]


def test_increment_clear_messages_unviewed_count(cm_dynamo, caplog):
    # add the chat to the DB, verify it is in DB
    chat_id, user_id = str(uuid4()), str(uuid4())
    transact = cm_dynamo.transact_add(chat_id, user_id)
    cm_dynamo.client.transact_write_items([transact])
    assert 'messagesUnviewedCount' not in cm_dynamo.get(chat_id, user_id)

    # increment
    assert cm_dynamo.increment_messages_unviewed_count(chat_id, user_id)['messagesUnviewedCount'] == 1
    assert cm_dynamo.get(chat_id, user_id)['messagesUnviewedCount'] == 1

    # increment
    assert cm_dynamo.increment_messages_unviewed_count(chat_id, user_id)['messagesUnviewedCount'] == 2
    assert cm_dynamo.get(chat_id, user_id)['messagesUnviewedCount'] == 2

    # clear
    assert 'messagesUnviewedCount' not in cm_dynamo.clear_messages_unviewed_count(chat_id, user_id)
    assert 'messagesUnviewedCount' not in cm_dynamo.get(chat_id, user_id)

    # check clear is idempotent
    assert 'messagesUnviewedCount' not in cm_dynamo.clear_messages_unviewed_count(chat_id, user_id)
    assert 'messagesUnviewedCount' not in cm_dynamo.get(chat_id, user_id)


@pytest.mark.parametrize(
    'incrementor_name, decrementor_name, attribute_name',
    [['increment_messages_unviewed_count', 'decrement_messages_unviewed_count', 'messagesUnviewedCount']],
)
def test_increment_decrement_count(cm_dynamo, caplog, incrementor_name, decrementor_name, attribute_name):
    incrementor = getattr(cm_dynamo, incrementor_name)
    decrementor = getattr(cm_dynamo, decrementor_name) if decrementor_name else None
    chat_id, user_id = str(uuid4()), str(uuid4())

    # can't increment message that doesnt exist
    with caplog.at_level(logging.WARNING):
        assert incrementor(chat_id, user_id) is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(x in caplog.records[0].msg for x in ['Failed to increment', attribute_name, chat_id, user_id])
    caplog.clear()

    # can't decrement message that doesnt exist
    if decrementor:
        with caplog.at_level(logging.WARNING):
            assert decrementor(chat_id, user_id) is None
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == 'WARNING'
        assert all(x in caplog.records[0].msg for x in ['Failed to decrement', attribute_name, chat_id, user_id])
        caplog.clear()

    # add the user to the DB, verify it is in DB
    transact = cm_dynamo.transact_add(chat_id, user_id)
    cm_dynamo.client.transact_write_items([transact])
    assert attribute_name not in cm_dynamo.get(chat_id, user_id)

    # increment twice, verify
    assert incrementor(chat_id, user_id)[attribute_name] == 1
    assert cm_dynamo.get(chat_id, user_id)[attribute_name] == 1
    assert incrementor(chat_id, user_id)[attribute_name] == 2
    assert cm_dynamo.get(chat_id, user_id)[attribute_name] == 2

    # all done if there's no decrementor method
    if not decrementor:
        return

    # decrement twice, verify
    assert decrementor(chat_id, user_id)[attribute_name] == 1
    assert cm_dynamo.get(chat_id, user_id)[attribute_name] == 1
    assert decrementor(chat_id, user_id)[attribute_name] == 0
    assert cm_dynamo.get(chat_id, user_id)[attribute_name] == 0

    # verify fail soft on trying to decrement below zero
    with caplog.at_level(logging.WARNING):
        resp = decrementor(chat_id, user_id)
    assert resp is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(x in caplog.records[0].msg for x in ['Failed to decrement', attribute_name, chat_id, user_id])
    assert cm_dynamo.get(chat_id, user_id)[attribute_name] == 0
