import json
import uuid
from unittest import mock

import pendulum
import pytest
from mock import patch

from app.models.block.enums import BlockStatus
from app.models.chat_message.exceptions import ChatMessageException
from app.models.post.enums import PostType


@pytest.fixture
def user1(user_manager, post_manager, grant_data_b64, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    user = user_manager.create_cognito_only_user(user_id, username)
    # give the user a profile photo so that it will show up in the message notification trigger calls
    post = post_manager.add_post(user, 'pid', PostType.IMAGE, image_input={'imageData': grant_data_b64})
    user.update_photo(post.id)
    yield user


@pytest.fixture
def user2(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def user3(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def chat(chat_manager, user1, user2):
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        yield chat_manager.add_direct_chat('cid', user1.id, user2.id)


@pytest.fixture
def message(chat_message_manager, chat, user1):
    yield chat_message_manager.add_chat_message('mid', 'lore ipsum', chat.id, user1.id)


@pytest.fixture
def system_message(chat_message_manager, chat):
    yield chat_message_manager.add_system_message(chat.id, 'system lore')


def test_chat_message_serialize(message, user1, user2, chat):
    # check that user1 has viewed it (since they wrote it) and user2 has not
    assert message.serialize(user1.id)['author']['blockerStatus'] == BlockStatus.SELF
    assert message.serialize(user2.id)['author']['blockerStatus'] == BlockStatus.NOT_BLOCKING


def test_chat_message_edit(message, user1, user2, card_manager):
    # check starting state
    assert message.item['text'] == 'lore ipsum'
    assert message.item['textTags'] == []
    assert 'lastEditedAt' not in message.item

    # edit the message
    username = user1.item['username']
    new_text = f'whats up with @{username}?'
    now = pendulum.now('utc')
    message.edit(new_text, now=now)
    assert message.item['text'] == new_text
    assert message.item['textTags'] == [{'tag': f'@{username}', 'userId': user1.id}]
    assert pendulum.parse(message.item['lastEditedAt']) == now

    # check state in dynamo
    message.refresh_item()
    assert message.item['text'] == new_text
    assert message.item['textTags'] == [{'tag': f'@{username}', 'userId': user1.id}]
    assert pendulum.parse(message.item['lastEditedAt']) == now


def test_chat_message_delete(message, user1, user2, card_manager):
    # check starting state
    assert message.refresh_item().item

    # do the delete, check final state
    message.delete()
    assert message.item  # keep in-memory copy of item around so we can serialize the gql response
    assert message.refresh_item().item is None


def test_get_author_encoded(chat_message_manager, user1, user2, user3, chat, block_manager):
    # regular message
    message = chat_message_manager.add_chat_message('mid', 'lore', chat.id, user1.id)
    author_encoded = message.get_author_encoded(user3.id)
    assert author_encoded
    author_serialized = json.loads(author_encoded)
    assert author_serialized['userId'] == user1.id
    assert author_serialized['username'] == user1.username
    assert author_serialized['photoPostId'] == user1.item['photoPostId']
    assert author_serialized['blockerStatus'] == 'NOT_BLOCKING'
    assert author_serialized['blockedStatus'] == 'NOT_BLOCKING'

    # add a blocking relationship
    block_manager.block(user1, user3)
    assert message.get_author_encoded(user3.id) is None

    # test the blocking relationship in the other direction
    message = chat_message_manager.add_chat_message('mid2', 'lore', chat.id, user3.id)
    assert message.get_author_encoded(user1.id) is None

    # test with no author (simulates a system message)
    message = chat_message_manager.add_chat_message('mid3', 'lore', chat.id, user2.id)
    assert message.get_author_encoded(user1.id)
    message._author = None
    assert message.get_author_encoded(user1.id) is None


def test_trigger_notifications_direct(message, chat, user1, user2, appsync_client):
    message.appsync = mock.Mock()
    message.trigger_notifications('ntype')
    assert message.appsync.mock_calls == [mock.call.trigger_notification('ntype', user2.id, message)]


def test_trigger_notifications_user_ids(message, chat, user1, user2, user3, appsync_client):
    # trigger a notification and check that we can use user_ids param to push
    # the notifications to users that aren't found in dynamo
    message.appsync = mock.Mock()
    message.trigger_notifications('ntype', user_ids=[user2.id, user3.id])
    assert message.appsync.mock_calls == [
        mock.call.trigger_notification('ntype', user2.id, message),
        mock.call.trigger_notification('ntype', user3.id, message),
    ]


def test_trigger_notifications_group(chat_manager, chat_message_manager, user1, user2, user3, appsync_client):
    # user1 creates a group chat with everyone in it
    group_chat = chat_manager.add_group_chat('cid', user1)
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        group_chat.add(user1, [user2.id, user3.id])

    # user2 creates a message, trigger notificaitons on it
    message_id = 'mid'
    message = chat_message_manager.add_chat_message(message_id, 'lore', group_chat.id, user2.id)
    message.appsync = mock.Mock()
    message.trigger_notifications('ntype')
    assert message.appsync.mock_calls == [
        mock.call.trigger_notification('ntype', user1.id, message),
        mock.call.trigger_notification('ntype', user3.id, message),
    ]

    # add system message, notifications are triggered automatically
    appsync_client.reset_mock()
    message = chat_message_manager.add_system_message_group_name_edited(group_chat.id, user3, 'cname')
    assert len(appsync_client.send.mock_calls) == 3  # one for each member of the group chat


def test_cant_flag_chat_message_of_chat_we_are_not_in(chat, message, user1, user2, user3):
    # user3 is not part of the chat, check they can't flag it
    with pytest.raises(ChatMessageException, match='User is not part of chat of message'):
        message.flag(user3)

    # can't flag system message
    org_user_id = message.user_id
    message.user_id = None
    with pytest.raises(ChatMessageException, match='Cannot flag system chat message'):
        message.flag(user2)
    message.user_id = org_user_id

    # verify user2 can flag without exception
    assert message.flag(user2)


def test_is_crowdsourced_forced_removal_criteria_met(message):
    # check starting state
    assert message.item.get('flagCount', 0) == 0
    assert message.chat.item.get('userCount', 0) == 2
    assert message.is_crowdsourced_forced_removal_criteria_met() is False

    # simulate a flag in a direct chat
    message.item['flagCount'] = 1
    assert message.is_crowdsourced_forced_removal_criteria_met() is True

    # simulate nine-person chat
    message.chat.item['userCount'] = 9
    message.item['flagCount'] = 0
    assert message.is_crowdsourced_forced_removal_criteria_met() is False
    message.item['flagCount'] = 1
    assert message.is_crowdsourced_forced_removal_criteria_met() is True

    # simulate ten-person chat
    message.chat.item['userCount'] = 10
    message.item['flagCount'] = 0
    assert message.is_crowdsourced_forced_removal_criteria_met() is False
    message.item['flagCount'] = 1
    assert message.is_crowdsourced_forced_removal_criteria_met() is False
    message.item['flagCount'] = 2
    assert message.is_crowdsourced_forced_removal_criteria_met() is True


def test_on_add_or_edit(message, system_message):
    # react to the edit of the normal message, verify
    with mock.patch.object(message.chat_manager, 'get_chat') as get_chat_mock:
        message.on_add_or_edit({'k': 'v'})
    assert get_chat_mock.mock_calls == []

    # react to the add of the normal message, verify
    with mock.patch.object(message.chat_manager, 'get_chat') as get_chat_mock:
        message.on_add_or_edit({})
    assert get_chat_mock.mock_calls == [
        mock.call(message.chat_id, strongly_consistent=True),
        mock.call().__bool__(),
        mock.call().on_message_add(message),
    ]

    # react to the add of the system message, verify
    with mock.patch.object(system_message.chat_manager, 'get_chat') as get_chat_mock:
        system_message.on_add_or_edit({})
    assert get_chat_mock.mock_calls == [
        mock.call(message.chat_id, strongly_consistent=True),
        mock.call().__bool__(),
        mock.call().on_message_add(system_message),
    ]
