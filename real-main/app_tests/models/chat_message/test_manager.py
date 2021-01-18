import uuid

import pendulum
import pytest
from mock import patch


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user
user3 = user


@pytest.fixture
def chat(chat_manager, user2, user3):
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        yield chat_manager.add_direct_chat(str(uuid.uuid4()), user2.id, user3.id)


def test_add_chat_message(chat_message_manager, chat, user, user2, user3):
    username = user.item['username']
    text = f'whats up with @{username}?'
    message_id = 'mid'
    user_id = 'uid'

    # add the message, check it looks ok
    now = pendulum.now('utc')
    now_str = now.to_iso8601_string()
    message = chat_message_manager.add_chat_message(message_id, text, chat.id, user_id, now=now)
    assert message.id == message_id
    assert message.user_id == user_id
    assert message.item['createdAt'] == now_str
    assert message.item['text'] == text
    assert message.item['textTags'] == [{'tag': f'@{username}', 'userId': user.id}]


def test_add_system_message(chat_message_manager, chat, appsync_client, user2, user3):
    text = 'sample sample'

    # add the message, check it looks ok
    now = pendulum.now('utc')
    message = chat_message_manager.add_system_message(chat.id, text, now=now)
    assert message.id
    assert message.user_id is None
    assert message.item['createdAt'] == now.to_iso8601_string()
    assert message.item['text'] == text
    assert message.item['textTags'] == []

    # check the chat message notifications were triggered correctly
    assert len(appsync_client.send.call_args_list) == 2
    assert len(appsync_client.send.call_args_list[0].args) == 2
    variables = appsync_client.send.call_args_list[0].args[1]
    assert variables['input']['userId'] == user2.id
    assert variables['input']['messageId'] == message.id
    assert variables['input']['authorUserId'] is None
    assert variables['input']['type'] == 'ADDED'
    assert len(appsync_client.send.call_args_list[1].args) == 2
    variables = appsync_client.send.call_args_list[1].args[1]
    assert variables['input']['userId'] == user3.id
    assert variables['input']['messageId'] == message.id
    assert variables['input']['authorUserId'] is None
    assert variables['input']['type'] == 'ADDED'


def test_add_system_message_group_created(chat_message_manager, chat, user):
    assert user.username

    # add the message, check it looks ok
    message = chat_message_manager.add_system_message_group_created(chat.id, user)
    assert message.item['text'] == f'@{user.username} created the group'
    assert message.item['textTags'] == [{'tag': f'@{user.username}', 'userId': user.id}]

    # add another message, check it looks ok
    message = chat_message_manager.add_system_message_group_created(chat.id, user, name='group name')
    assert message.item['text'] == f'@{user.username} created the group "group name"'
    assert message.item['textTags'] == [{'tag': f'@{user.username}', 'userId': user.id}]


def test_add_system_message_added_to_group(chat_message_manager, chat, user, user2, user3):
    assert user.username
    assert user2.username
    assert user3.username

    # can't add no users
    with pytest.raises(AssertionError):
        chat_message_manager.add_system_message_added_to_group(chat.id, user, [])

    # add one user
    message = chat_message_manager.add_system_message_added_to_group(chat.id, user, [user2])
    assert message.item['text'] == f'@{user.username} added @{user2.username} to the group'
    assert len(message.item['textTags']) == 2

    # add two users
    message = chat_message_manager.add_system_message_added_to_group(chat.id, user, [user2, user3])
    assert message.item['text'] == f'@{user.username} added @{user2.username} and @{user3.username} to the group'
    assert len(message.item['textTags']) == 3

    # add three users
    message = chat_message_manager.add_system_message_added_to_group(chat.id, user, [user2, user3, user])
    assert (
        message.item['text']
        == f'@{user.username} added @{user2.username}, @{user3.username} and @{user.username} to the group'
    )
    assert len(message.item['textTags']) == 3


def test_add_system_message_left_group(chat_message_manager, chat, user):
    assert user.username

    # user leaves
    message = chat_message_manager.add_system_message_left_group(chat.id, user)
    assert message.item['text'] == f'@{user.username} left the group'
    assert len(message.item['textTags']) == 1


def test_add_system_message_group_name_edited(chat_message_manager, chat, user):
    assert user.username

    # user changes the name
    message = chat_message_manager.add_system_message_group_name_edited(chat.id, user, '4eva')
    assert message.item['text'] == f'@{user.username} changed the name of the group to "4eva"'
    assert len(message.item['textTags']) == 1

    # user deletes the name the name
    message = chat_message_manager.add_system_message_group_name_edited(chat.id, user, None)
    assert message.item['text'] == f'@{user.username} deleted the name of the group'
    assert len(message.item['textTags']) == 1
