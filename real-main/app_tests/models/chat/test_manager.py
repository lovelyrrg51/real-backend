import logging
import uuid
from unittest import mock

import pendulum
import pytest
from mock import patch

from app.mixins.view.enums import ViewedStatus
from app.models.chat.enums import ChatType
from app.models.chat.exceptions import ChatException
from app.models.user.enums import UserStatus


@pytest.fixture
def user1(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user1
user3 = user1


def test_cant_add_direct_chat_blocked(chat_manager, block_manager, user1, user2):
    # user1 blocks user2
    assert block_manager.block(user1, user2)
    chat_id = 'cid'

    with pytest.raises(ChatException, match='has blocked'):
        chat_manager.add_direct_chat(chat_id, user1.id, user2.id)

    with pytest.raises(ChatException, match='has been blocked'):
        chat_manager.add_direct_chat(chat_id, user2.id, user1.id)


def test_cant_add_direct_chat_anonymous(chat_manager, block_manager, user1, user2):
    # set user2 up as anonymous
    user2.dynamo.set_user_status(user2.id, UserStatus.ANONYMOUS)
    assert user2.refresh_item().status == UserStatus.ANONYMOUS

    chat_id = 'cid'
    with pytest.raises(ChatException, match='with status'):
        chat_manager.add_direct_chat(chat_id, user1.id, user2.id)


def test_cant_add_direct_chat_with_self(chat_manager, user1):
    chat_id = 'cid'
    with pytest.raises(ChatException, match='with themselves'):
        chat_manager.add_direct_chat(chat_id, user1.id, user1.id)


def test_cant_add_direct_chat_already_exists(chat_manager, user1, user2):
    # add the chat
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        chat_manager.add_direct_chat('cid3', user1.id, user2.id)

    # verify can't add it again, even with new chat id
    with pytest.raises(ChatException, match='already exists'):
        chat_manager.add_direct_chat('cid4', user1.id, user2.id)
    with pytest.raises(ChatException, match='already exists'):
        chat_manager.add_direct_chat('cid4', user2.id, user1.id)


def test_add_direct_chat(chat_manager, user1, user2):
    now = pendulum.now('utc')

    # add the chat, verify it looks ok
    chat_id = 'cid'
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        chat = chat_manager.add_direct_chat(chat_id, user1.id, user2.id, now=now)
    assert chat.id == chat_id
    assert chat.type == ChatType.DIRECT
    assert chat.item['createdAt'] == now.to_iso8601_string()
    assert chat.item['createdByUserId'] == user1.id
    assert chat.item.get('messagesCount', 0) == 0
    assert chat.item.get('name') is None

    # verify chat memberships for both users were created
    user_ids = chat_manager.member_dynamo.generate_user_ids_by_chat(chat_id)
    assert sorted(user_ids) == sorted([user1.id, user2.id])


def test_add_minimal_group_chat(chat_manager, user1):
    # verify and set up starting state
    chat_manager.chat_message_manager = mock.Mock()

    # add the chat, verify it looks ok
    chat_id = 'cid'
    before = pendulum.now('utc')
    chat = chat_manager.add_group_chat(chat_id, user1)
    after = pendulum.now('utc')
    assert chat.id == chat_id
    assert chat.type == ChatType.GROUP
    assert chat.item.get('messagesCount', 0) == 0
    assert chat.item.get('name') is None
    assert chat.item['createdByUserId'] == user1.id
    created_at = pendulum.parse(chat.item['createdAt'])
    assert before <= created_at
    assert after >= created_at

    # verify chat membership was created
    user_ids = list(chat_manager.member_dynamo.generate_user_ids_by_chat(chat_id))
    assert user_ids == [user1.id]

    # verify the system chat message was triggered
    assert chat_manager.chat_message_manager.mock_calls == [
        mock.call.add_system_message_group_created(chat_id, user1, name=None, now=created_at),
    ]


def test_add_maximal_group_chat(chat_manager, user1):
    # verify and set up starting state
    chat_manager.chat_message_manager = mock.Mock()

    # add the chat, verify it looks ok
    chat_id = 'cid'
    name = 'lore'
    now = pendulum.now('utc')
    chat = chat_manager.add_group_chat(chat_id, user1, name=name, now=now)
    assert chat.id == chat_id
    assert chat.type == ChatType.GROUP
    assert chat.item.get('messagesCount', 0) == 0
    assert chat.item['name'] == name
    assert chat.item['createdByUserId'] == user1.id
    assert pendulum.parse(chat.item['createdAt']) == now

    # verify chat membership was created
    user_ids = list(chat_manager.member_dynamo.generate_user_ids_by_chat(chat_id))
    assert user_ids == [user1.id]

    # verify the system chat message was triggered
    assert chat_manager.chat_message_manager.mock_calls == [
        mock.call.add_system_message_group_created(chat_id, user1, name=name, now=now),
    ]


def test_record_views(chat_manager, user1, user2, user3, caplog):
    chat_id = str(uuid.uuid4())

    # verify can't record views on chat that DNE
    with caplog.at_level(logging.WARNING):
        chat_manager.record_views([chat_id], user1.id)
    assert len(caplog.records) == 1
    assert 'Cannot record view' in caplog.records[0].msg
    assert 'on DNE chat' in caplog.records[0].msg
    assert chat_id in caplog.records[0].msg
    assert user1.id in caplog.records[0].msg

    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        chat = chat_manager.add_direct_chat(chat_id, user1.id, user2.id)
    assert chat.get_viewed_status(user1.id) == ViewedStatus.NOT_VIEWED
    assert chat.get_viewed_status(user2.id) == ViewedStatus.NOT_VIEWED
    assert chat.get_viewed_status(user3.id) == ViewedStatus.NOT_VIEWED

    # verify non-member can't record views on chat
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        chat_manager.record_views([chat_id], user3.id)
    assert len(caplog.records) == 1
    assert 'Cannot record view' in caplog.records[0].msg
    assert 'by non-member user' in caplog.records[0].msg
    assert chat_id in caplog.records[0].msg
    assert user3.id in caplog.records[0].msg
    assert chat.get_viewed_status(user3.id) == ViewedStatus.NOT_VIEWED

    # verify member can record views on chat
    caplog.clear()
    chat_manager.record_views([chat_id], user1.id)
    assert caplog.records == []
    assert chat.get_viewed_status(user1.id) == ViewedStatus.VIEWED
