from uuid import uuid4

import pendulum
import pytest
from mock import patch


@pytest.fixture
def user1(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user1


@pytest.fixture
def chat(chat_manager, user1, user2):
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        yield chat_manager.add_direct_chat(str(uuid4()), user1.id, user2.id)


def test_sync_member_messages_unviewed_count(chat_manager, chat, user1, user2):
    # add some counts for each member of the chat, verify
    chat_manager.member_dynamo.increment_messages_unviewed_count(chat.id, user1.id)
    chat_manager.member_dynamo.increment_messages_unviewed_count(chat.id, user1.id)
    chat_manager.member_dynamo.increment_messages_unviewed_count(chat.id, user2.id)
    chat_manager.member_dynamo.increment_messages_unviewed_count(chat.id, user2.id)
    assert chat_manager.member_dynamo.get(chat.id, user1.id).get('messagesUnviewedCount', 0) == 2
    assert chat_manager.member_dynamo.get(chat.id, user2.id).get('messagesUnviewedCount', 0) == 2

    # check adding a view clears the count
    view_item = chat_manager.view_dynamo.add_view(chat.id, user1.id, 1, pendulum.now('utc'))
    chat_manager.sync_member_messages_unviewed_count(chat.id, view_item, {})
    assert chat_manager.member_dynamo.get(chat.id, user1.id).get('messagesUnviewedCount', 0) == 0

    # check an unchanged view count makes no changes
    view_item = chat_manager.view_dynamo.add_view(chat.id, user2.id, 2, pendulum.now('utc'))
    chat_manager.sync_member_messages_unviewed_count(chat.id, view_item, view_item)
    assert chat_manager.member_dynamo.get(chat.id, user2.id).get('messagesUnviewedCount', 0) == 2

    # check an incremented view count clears the count
    new_view_item = chat_manager.view_dynamo.increment_view_count(chat.id, user2.id, 3, pendulum.now('utc'))
    chat_manager.sync_member_messages_unviewed_count(chat.id, new_view_item, view_item)
    assert chat_manager.member_dynamo.get(chat.id, user2.id).get('messagesUnviewedCount', 0) == 0
