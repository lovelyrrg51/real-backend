import logging
from unittest.mock import patch
from uuid import uuid4

import pendulum
import pytest


@pytest.fixture
def user1(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user1
user3 = user1


@pytest.fixture
def chat(chat_manager, user1, user2):
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        yield chat_manager.add_direct_chat(str(uuid4()), user1.id, user2.id)


@pytest.fixture
def user1_message(chat_message_manager, chat, user1):
    yield chat_message_manager.add_chat_message(str(uuid4()), 'lore', chat.id, user1.id)


@pytest.fixture
def user2_message(chat_message_manager, chat, user2):
    yield chat_message_manager.add_chat_message(str(uuid4()), 'lore', chat.id, user2.id)


@pytest.fixture
def system_message(chat_message_manager, chat):
    yield chat_message_manager.add_system_message(chat.id, 'system lore')


def test_on_message_added(chat_manager, chat, user1, user2, caplog, user1_message, user2_message):
    # verify starting state
    chat.refresh_item()
    assert 'messagesCount' not in chat.item
    assert 'lastMessageActivityAt' not in chat.item
    user1_member_item = chat.member_dynamo.get(chat.id, user1.id)
    user2_member_item = chat.member_dynamo.get(chat.id, user2.id)
    assert user1_member_item['gsiK2SortKey'].split('/') == ['chat', chat.item['createdAt']]
    assert user2_member_item['gsiK2SortKey'].split('/') == ['chat', chat.item['createdAt']]
    assert 'messagesUnviewedCount' not in user1_member_item
    assert 'messagesUnviewedCount' not in user2_member_item

    # react to adding a message by user1, verify state
    now = user1_message.created_at
    chat_manager.on_chat_message_add(user1_message.id, new_item=user1_message.item)
    chat.refresh_item()
    assert chat.item['messagesCount'] == 1
    assert pendulum.parse(chat.item['lastMessageActivityAt']) == now
    user1_member_item = chat.member_dynamo.get(chat.id, user1.id)
    user2_member_item = chat.member_dynamo.get(chat.id, user2.id)
    assert user1_member_item['gsiK2SortKey'].split('/') == ['chat', now.to_iso8601_string()]
    assert user2_member_item['gsiK2SortKey'].split('/') == ['chat', now.to_iso8601_string()]
    assert 'messagesUnviewedCount' not in user1_member_item
    assert user2_member_item['messagesUnviewedCount'] == 1

    # react to adding a message by user2, verify state
    now = user2_message.created_at
    chat_manager.on_chat_message_add(user2_message.id, new_item=user2_message.item)
    chat.refresh_item()
    assert chat.item['messagesCount'] == 2
    assert pendulum.parse(chat.item['lastMessageActivityAt']) == now
    user1_member_item = chat.member_dynamo.get(chat.id, user1.id)
    user2_member_item = chat.member_dynamo.get(chat.id, user2.id)
    assert user1_member_item['gsiK2SortKey'].split('/') == ['chat', now.to_iso8601_string()]
    assert user2_member_item['gsiK2SortKey'].split('/') == ['chat', now.to_iso8601_string()]
    assert user1_member_item['messagesUnviewedCount'] == 1
    assert user2_member_item['messagesUnviewedCount'] == 1

    # react to adding a another message by user2 out of order
    new_item = {
        **user2_message.item,
        'createdAt': user2_message.created_at.subtract(seconds=5).to_iso8601_string(),
    }
    with caplog.at_level(logging.WARNING):
        chat_manager.on_chat_message_add(user2_message.id, new_item=new_item)
    assert len(caplog.records) == 3
    assert all('Failed' in rec.msg for rec in caplog.records)
    assert all('last message activity' in rec.msg for rec in caplog.records)
    assert all(chat.id in rec.msg for rec in caplog.records)
    assert user1.id in caplog.records[1].msg
    assert user2.id in caplog.records[2].msg

    # verify final state
    chat.refresh_item()
    assert chat.item['messagesCount'] == 3
    assert pendulum.parse(chat.item['lastMessageActivityAt']) == now
    user1_member_item = chat.member_dynamo.get(chat.id, user1.id)
    user2_member_item = chat.member_dynamo.get(chat.id, user2.id)
    assert user1_member_item['gsiK2SortKey'].split('/') == ['chat', now.to_iso8601_string()]
    assert user2_member_item['gsiK2SortKey'].split('/') == ['chat', now.to_iso8601_string()]
    assert user1_member_item['messagesUnviewedCount'] == 2
    assert user2_member_item['messagesUnviewedCount'] == 1


def test_on_message_added_system_message(chat_manager, chat, user1, user2, system_message):
    # verify starting state
    chat.refresh_item()
    assert 'messagesCount' not in chat.item
    assert 'lastMessageActivityAt' not in chat.item
    user1_member_item = chat.member_dynamo.get(chat.id, user1.id)
    user2_member_item = chat.member_dynamo.get(chat.id, user2.id)
    assert user1_member_item['gsiK2SortKey'].split('/') == ['chat', chat.item['createdAt']]
    assert user2_member_item['gsiK2SortKey'].split('/') == ['chat', chat.item['createdAt']]
    assert 'messagesUnviewedCount' not in user1_member_item
    assert 'messagesUnviewedCount' not in user2_member_item

    # react to adding a message by the system, verify state
    now = system_message.created_at
    chat_manager.on_chat_message_add(system_message.id, new_item=system_message.item)
    chat.refresh_item()
    assert chat.item['messagesCount'] == 1
    assert pendulum.parse(chat.item['lastMessageActivityAt']) == now
    user1_member_item = chat.member_dynamo.get(chat.id, user1.id)
    user2_member_item = chat.member_dynamo.get(chat.id, user2.id)
    assert user1_member_item['gsiK2SortKey'].split('/') == ['chat', now.to_iso8601_string()]
    assert user2_member_item['gsiK2SortKey'].split('/') == ['chat', now.to_iso8601_string()]
    assert user1_member_item['messagesUnviewedCount'] == 1
    assert user2_member_item['messagesUnviewedCount'] == 1


def test_on_chat_message_delete(chat_manager, chat, user1, user2, caplog, user1_message):
    # reacht to an add to increment counts, and verify starting state
    chat_manager.on_chat_message_add(user1_message.id, new_item=user1_message.item)
    assert chat.refresh_item().item['messagesCount'] == 1
    assert chat.member_dynamo.get(chat.id, user1.id).get('messagesUnviewedCount', 0) == 0
    assert chat.member_dynamo.get(chat.id, user2.id).get('messagesUnviewedCount', 0) == 1

    # react to a message delete, verify counts drop as expected
    chat_manager.on_chat_message_delete(user1_message.id, old_item=user1_message.item)
    assert chat.refresh_item().item['messagesCount'] == 0
    assert chat.member_dynamo.get(chat.id, user1.id).get('messagesUnviewedCount', 0) == 0
    assert chat.member_dynamo.get(chat.id, user2.id).get('messagesUnviewedCount', 0) == 0

    # react to a message delete, verify fails softly and final state
    with caplog.at_level(logging.WARNING):
        chat_manager.on_chat_message_delete(user1_message.id, old_item=user1_message.item)
    assert len(caplog.records) == 2
    assert 'Failed to decrement messagesCount' in caplog.records[0].msg
    assert 'Failed to decrement messagesUnviewedCount' in caplog.records[1].msg
    assert chat.id in caplog.records[0].msg
    assert chat.id in caplog.records[1].msg
    assert chat.refresh_item().item['messagesCount'] == 0
    assert chat.member_dynamo.get(chat.id, user1.id).get('messagesUnviewedCount', 0) == 0
    assert chat.member_dynamo.get(chat.id, user2.id).get('messagesUnviewedCount', 0) == 0


def test_on_message_delete_handles_chat_views_correctly(chat, user1, user2, chat_message_manager, chat_manager):
    # each user posts two messages, one of which is 'viewed' by both and the other is not
    message1 = chat_message_manager.add_chat_message(str(uuid4()), 'lore ipsum', chat.id, user1.id)
    message2 = chat_message_manager.add_chat_message(str(uuid4()), 'lore ipsum', chat.id, user2.id)
    chat_manager.on_chat_message_add(message1.id, new_item=message1.item)
    chat_manager.on_chat_message_add(message1.id, new_item=message1.item)

    chat_manager.record_views([chat.id], user1.id)
    chat_manager.record_views([chat.id], user2.id)
    chat_manager.member_dynamo.clear_messages_unviewed_count(chat.id, user1.id)
    chat_manager.member_dynamo.clear_messages_unviewed_count(chat.id, user2.id)

    message3 = chat_message_manager.add_chat_message(str(uuid4()), 'lore ipsum', chat.id, user1.id)
    message4 = chat_message_manager.add_chat_message(str(uuid4()), 'lore ipsum', chat.id, user2.id)
    chat_manager.on_chat_message_add(message3.id, new_item=message3.item)
    chat_manager.on_chat_message_add(message4.id, new_item=message4.item)

    # verify starting state
    chat.refresh_item()
    assert chat.item['messagesCount'] == 4
    assert pendulum.parse(chat.item['lastMessageActivityAt']) == message4.created_at
    assert chat.member_dynamo.get(chat.id, user1.id)['messagesUnviewedCount'] == 1
    assert chat.member_dynamo.get(chat.id, user2.id)['messagesUnviewedCount'] == 1

    # react to deleting message2, check counts
    chat_manager.on_chat_message_delete(message2.id, old_item=message2.item)
    assert chat.refresh_item().item['messagesCount'] == 3
    assert chat.member_dynamo.get(chat.id, user1.id)['messagesUnviewedCount'] == 1
    assert chat.member_dynamo.get(chat.id, user2.id)['messagesUnviewedCount'] == 1

    # react to deleting message3, check counts
    chat_manager.on_chat_message_delete(message3.id, old_item=message3.item)
    assert chat.refresh_item().item['messagesCount'] == 2
    assert chat.member_dynamo.get(chat.id, user1.id)['messagesUnviewedCount'] == 1
    assert chat.member_dynamo.get(chat.id, user2.id)['messagesUnviewedCount'] == 0

    # react to deleting message1, check counts
    chat_manager.on_chat_message_delete(message1.id, old_item=message1.item)
    assert chat.refresh_item().item['messagesCount'] == 1
    assert chat.member_dynamo.get(chat.id, user1.id)['messagesUnviewedCount'] == 1
    assert chat.member_dynamo.get(chat.id, user2.id)['messagesUnviewedCount'] == 0

    # react to deleting message4, check counts
    chat_manager.on_chat_message_delete(message4.id, old_item=message4.item)
    assert chat.refresh_item().item['messagesCount'] == 0
    assert chat.member_dynamo.get(chat.id, user1.id)['messagesUnviewedCount'] == 0
    assert chat.member_dynamo.get(chat.id, user2.id)['messagesUnviewedCount'] == 0


def test_on_flag_add_deletes_chat_if_crowdsourced_criteria_met(chat_manager, chat, user2):
    # react to a flagging without meeting the criteria, verify doesn't delete
    with patch.object(chat, 'is_crowdsourced_forced_removal_criteria_met', return_value=False):
        with patch.object(chat_manager, 'init_chat', return_value=chat):
            chat_manager.on_flag_add(chat.id, new_item={})
    assert chat.refresh_item().item

    # react to a flagging with meeting the criteria, verify deletes
    with patch.object(chat, 'is_crowdsourced_forced_removal_criteria_met', return_value=True):
        with patch.object(chat_manager, 'init_chat', return_value=chat):
            chat_manager.on_flag_add(chat.id, new_item={})
    assert chat.refresh_item().item is None


def test_on_chat_delete_delete_memberships(chat_manager, user1, user2, chat):
    # set up a group chat as well, add both users, verify starting state
    group_chat = chat_manager.add_group_chat(str(uuid4()), user1)
    group_chat.add(user1, [user2.id])
    assert sum(1 for _ in chat_manager.member_dynamo.generate_chat_ids_by_user(user1.id)) == 2
    assert sum(1 for _ in chat_manager.member_dynamo.generate_chat_ids_by_user(user2.id)) == 2

    # react to the delete of one of the chats, verify state
    chat_manager.on_chat_delete_delete_memberships(chat.id, old_item=chat.item)
    assert sum(1 for _ in chat_manager.member_dynamo.generate_chat_ids_by_user(user1.id)) == 1
    assert sum(1 for _ in chat_manager.member_dynamo.generate_chat_ids_by_user(user2.id)) == 1

    # react to the delete of the other chat, verify state
    chat_manager.on_chat_delete_delete_memberships(group_chat.id, old_item=group_chat.item)
    assert sum(1 for _ in chat_manager.member_dynamo.generate_chat_ids_by_user(user1.id)) == 0
    assert sum(1 for _ in chat_manager.member_dynamo.generate_chat_ids_by_user(user2.id)) == 0


def test_on_user_delete_leave_all_chats(chat_manager, user1, user2, user3):
    # user1 opens up direct chats with both of the other two users
    chat_id_1 = 'cid1'
    chat_id_2 = 'cid2'
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        chat_manager.add_direct_chat(chat_id_1, user1.id, user2.id)
        chat_manager.add_direct_chat(chat_id_2, user1.id, user3.id)

    # user1 sets up a group chat with only themselves in it, and another with user2
    chat_id_3 = 'cid3'
    chat_id_4 = 'cid4'
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        chat_manager.add_group_chat(chat_id_3, user1)
        chat_manager.add_group_chat(chat_id_4, user1).add(user1, [user2.id])

    # verify we see the chat and chat_memberships in the DB
    assert chat_manager.dynamo.get(chat_id_1)['userCount'] == 2
    assert chat_manager.dynamo.get(chat_id_2)['userCount'] == 2
    assert chat_manager.dynamo.get(chat_id_3)['userCount'] == 1
    assert chat_manager.dynamo.get(chat_id_4)['userCount'] == 2
    assert chat_manager.member_dynamo.get(chat_id_1, user1.id)
    assert chat_manager.member_dynamo.get(chat_id_1, user2.id)
    assert chat_manager.member_dynamo.get(chat_id_2, user1.id)
    assert chat_manager.member_dynamo.get(chat_id_2, user3.id)
    assert chat_manager.member_dynamo.get(chat_id_3, user1.id)
    assert chat_manager.member_dynamo.get(chat_id_4, user1.id)
    assert chat_manager.member_dynamo.get(chat_id_4, user2.id)

    # user1 leaves all their chats, which should trigger deletes of both direct chats
    chat_manager.on_user_delete_leave_all_chats(user1.id, old_item=user1.item)

    # verify we see the chat and chat_memberships in the DB
    assert chat_manager.dynamo.get(chat_id_1) is None
    assert chat_manager.dynamo.get(chat_id_2) is None
    assert chat_manager.dynamo.get(chat_id_3) is None
    assert chat_manager.dynamo.get(chat_id_4)['userCount'] == 1
    assert chat_manager.member_dynamo.get(chat_id_3, user1.id) is None
    assert chat_manager.member_dynamo.get(chat_id_4, user1.id) is None
    assert chat_manager.member_dynamo.get(chat_id_4, user2.id)
