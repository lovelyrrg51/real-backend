import json
import uuid

import pytest
from mock import patch

from app.models.chat_message.appsync import ChatMessageAppSync
from app.models.post.enums import PostType


@pytest.fixture
def chat_message_appsync(appsync_client):
    yield ChatMessageAppSync(appsync_client)


@pytest.fixture
def user1(user_manager, post_manager, image_data_b64, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    user = user_manager.create_cognito_only_user(user_id, username)
    # give the user a profile photo so that it will show up in the message notification trigger calls
    post = post_manager.add_post(user, 'pid', PostType.IMAGE, image_input={'imageData': image_data_b64})
    user.update_photo(post.id)
    yield user


@pytest.fixture
def user2(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def chat(chat_manager, user1, user2):
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        yield chat_manager.add_direct_chat('cid', user1.id, user2.id)


@pytest.fixture
def message(chat_message_manager, chat, user1):
    message_id = 'mid'
    text = 'lore ipsum'
    yield chat_message_manager.add_chat_message(message_id, text, chat.id, user1.id)


def test_trigger_notification(chat_message_appsync, message, chat, user1, user2, appsync_client):
    appsync_client.reset_mock()

    # trigger a notificaiton and check our mock client was called as expected
    chat_message_appsync.trigger_notification('ntype', user2.id, message)
    assert len(appsync_client.mock_calls) == 1
    assert len(appsync_client.send.call_args.kwargs) == 0
    assert len(appsync_client.send.call_args.args) == 2
    mutation, variables = appsync_client.send.call_args.args
    assert 'triggerChatMessageNotification' in str(mutation)
    assert list(variables.keys()) == ['input']
    assert len(variables['input']) == 10
    assert variables['input']['userId'] == user2.id
    assert variables['input']['messageId'] == 'mid'
    assert variables['input']['chatId'] == chat.id
    assert variables['input']['authorUserId'] == user1.id
    assert json.loads(variables['input']['authorEncoded'])['userId'] == user1.id
    assert json.loads(variables['input']['authorEncoded'])['username'] == user1.username
    assert variables['input']['type'] == 'ntype'
    assert variables['input']['text'] == message.item['text']
    assert variables['input']['textTaggedUserIds'] == []
    assert variables['input']['createdAt'] == message.item['createdAt']
    assert variables['input']['lastEditedAt'] is None


def test_trigger_notification_blocking_relationship(
    chat_message_appsync, chat_message_manager, chat, user1, user2, appsync_client, block_manager
):
    # user1 triggers a message notification that user2 recieves (in a group chat)
    message1 = chat_message_manager.add_chat_message('mid3', 'lore', chat.id, user1.id)
    message2 = chat_message_manager.add_chat_message('mid4', 'lore', chat.id, user2.id)

    # user1 blocks user2
    block_manager.block(user1, user2)

    chat_message_appsync.trigger_notification('ntype', user2.id, message1)
    assert appsync_client.send.call_args.args[1]['input']['userId'] == user2.id
    assert appsync_client.send.call_args.args[1]['input']['authorUserId'] == user1.id
    assert appsync_client.send.call_args.args[1]['input']['authorEncoded'] is None

    chat_message_appsync.trigger_notification('ntype', user1.id, message2)
    assert appsync_client.send.call_args.args[1]['input']['userId'] == user1.id
    assert appsync_client.send.call_args.args[1]['input']['authorUserId'] == user2.id
    assert appsync_client.send.call_args.args[1]['input']['authorEncoded'] is None


def test_trigger_notification_system_message(
    chat_message_appsync, chat_manager, chat_message_manager, user1, appsync_client
):
    group_chat = chat_manager.add_group_chat('cid', user1)
    appsync_client.reset_mock()
    # adding a system message triggers the notifcations automatically
    message = chat_message_manager.add_system_message_group_name_edited(group_chat.id, user1, 'cname')
    assert len(appsync_client.mock_calls) == 1
    assert len(appsync_client.send.call_args.kwargs) == 0
    assert len(appsync_client.send.call_args.args) == 2
    mutation, variables = appsync_client.send.call_args.args
    assert 'triggerChatMessageNotification' in str(mutation)
    assert list(variables.keys()) == ['input']
    assert len(variables['input']) == 10
    assert variables['input']['userId'] == user1.id
    assert variables['input']['messageId'] == message.id
    assert variables['input']['chatId'] == group_chat.id
    assert variables['input']['authorUserId'] is None
    assert variables['input']['authorEncoded'] is None
    assert variables['input']['type'] == 'ADDED'
    assert variables['input']['text'] == message.item['text']
    assert variables['input']['textTaggedUserIds'] == [{'tag': f'@{user1.username}', 'userId': user1.id}]
    assert variables['input']['createdAt'] == message.item['createdAt']
    assert variables['input']['lastEditedAt'] is None
