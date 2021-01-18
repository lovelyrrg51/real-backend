import logging
from unittest.mock import call, patch
from uuid import uuid4

import pytest

from app.models.follower.enums import FollowStatus
from app.models.user.enums import UserPrivacyStatus, UserStatus


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user


@pytest.fixture
def chat(chat_manager, user, user2):
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        yield chat_manager.add_direct_chat(str(uuid4()), user.id, user2.id)


@pytest.fixture
def message(chat_message_manager, chat, user2):
    yield chat_message_manager.add_chat_message(str(uuid4()), 'lore ipsum', chat.id, user2.id)


@pytest.fixture
def system_message(chat_message_manager, chat):
    yield chat_message_manager.add_system_message(chat.id, 'system lore')


def test_sync_elasticsearch(user_manager, user):
    with patch.object(user_manager, 'elasticsearch_client') as elasticsearch_client_mock:
        user_manager.sync_elasticsearch(user.id, {'username': 'spock'}, 'garbage')
    assert elasticsearch_client_mock.mock_calls == [call.put_user(user.id, 'spock', None)]

    with patch.object(user_manager, 'elasticsearch_client') as elasticsearch_client_mock:
        user_manager.sync_elasticsearch(user.id, {'username': 'sp', 'fullName': 'fn'}, 'garbage')
    assert elasticsearch_client_mock.mock_calls == [call.put_user(user.id, 'sp', 'fn')]


@pytest.mark.parametrize(
    'method_name, pinpoint_attribute, dynamo_attribute',
    [['sync_pinpoint_email', 'EMAIL', 'email'], ['sync_pinpoint_phone', 'SMS', 'phoneNumber']],
)
def test_sync_pinpoint_attribute(user_manager, user, method_name, pinpoint_attribute, dynamo_attribute):
    # test no value
    user.item.pop(dynamo_attribute, None)
    with patch.object(user_manager, 'pinpoint_client') as pinpoint_client_mock:
        getattr(user_manager, method_name)(user.id, user.item, user.item)
    assert pinpoint_client_mock.mock_calls == [call.delete_user_endpoint(user.id, pinpoint_attribute)]

    # test with value
    user.item[dynamo_attribute] = 'the-val'
    with patch.object(user_manager, 'pinpoint_client') as pinpoint_client_mock:
        getattr(user_manager, method_name)(user.id, user.item, user.item)
    assert pinpoint_client_mock.mock_calls == [call.update_user_endpoint(user.id, pinpoint_attribute, 'the-val')]


def test_sync_pinpoint_user_status(user_manager, user):
    user.item['userStatus'] = UserStatus.ACTIVE
    with patch.object(user_manager, 'pinpoint_client') as pinpoint_client_mock:
        user_manager.sync_pinpoint_user_status(user.id, user.item, user.item)
    assert pinpoint_client_mock.mock_calls == [call.enable_user_endpoints(user.id)]

    user.item['userStatus'] = UserStatus.DISABLED
    with patch.object(user_manager, 'pinpoint_client') as pinpoint_client_mock:
        user_manager.sync_pinpoint_user_status(user.id, user.item, user.item)
    assert pinpoint_client_mock.mock_calls == [call.disable_user_endpoints(user.id)]

    user.item['userStatus'] = UserStatus.DELETING
    with patch.object(user_manager, 'pinpoint_client') as pinpoint_client_mock:
        user_manager.sync_pinpoint_user_status(user.id, user.item, user.item)
    assert pinpoint_client_mock.mock_calls == [call.delete_user_endpoints(user.id)]


def test_sync_chats_with_unviewed_messages_count_chat_member_added(user_manager, chat, user):
    assert user.refresh_item().item.get('chatsWithUnviewedMessagesCount', 0) == 0

    # sync add of member with no unviewed message count, verify
    new_item = chat.member_dynamo.get(chat.id, user.id)
    assert 'messagesUnviewedCount' not in new_item
    user_manager.sync_chats_with_unviewed_messages_count(chat.id, new_item=new_item, old_item={})
    assert user.refresh_item().item.get('chatsWithUnviewedMessagesCount', 0) == 0

    # synd add of member with some unviewed message count, verify
    new_item = chat.member_dynamo.increment_messages_unviewed_count(chat.id, user.id)
    assert new_item['messagesUnviewedCount'] == 1
    user_manager.sync_chats_with_unviewed_messages_count(chat.id, new_item=new_item, old_item={})


def test_sync_chats_with_unviewed_messages_count_chat_member_edited(user_manager, chat, user):
    assert user.refresh_item().item.get('chatsWithUnviewedMessagesCount', 0) == 0

    # sync edit of member from no unviewed message count to some, verify
    item1 = chat.member_dynamo.get(chat.id, user.id)
    assert 'messagesUnviewedCount' not in item1
    item2 = chat.member_dynamo.increment_messages_unviewed_count(chat.id, user.id)
    assert item2['messagesUnviewedCount'] == 1
    user_manager.sync_chats_with_unviewed_messages_count(chat.id, new_item=item2, old_item=item1)
    assert user.refresh_item().item.get('chatsWithUnviewedMessagesCount', 0) == 1

    # sync edit of member from some unviewed message count some more, verify
    item3 = chat.member_dynamo.increment_messages_unviewed_count(chat.id, user.id)
    assert item3['messagesUnviewedCount'] == 2
    user_manager.sync_chats_with_unviewed_messages_count(chat.id, new_item=item3, old_item=item2)
    assert user.refresh_item().item.get('chatsWithUnviewedMessagesCount', 0) == 1

    # sync edit of member from some unviewed message count to none, verify
    user_manager.sync_chats_with_unviewed_messages_count(chat.id, new_item=item1, old_item=item3)
    assert user.refresh_item().item.get('chatsWithUnviewedMessagesCount', 0) == 0


def test_sync_chats_with_unviewed_messages_count_chat_member_deleted(user_manager, chat, user, caplog):
    user.dynamo.increment_chats_with_unviewed_messages_count(user.id)
    assert user.refresh_item().item.get('chatsWithUnviewedMessagesCount', 0) == 1

    # sync delete of member with no unviewed message count, verify
    old_item = chat.member_dynamo.get(chat.id, user.id)
    assert 'messagesUnviewedCount' not in old_item
    user_manager.sync_chats_with_unviewed_messages_count(chat.id, new_item={}, old_item=old_item)
    assert user.refresh_item().item.get('chatsWithUnviewedMessagesCount', 0) == 1

    # sync delete of member with some unviewed message count, verify
    old_item = chat.member_dynamo.increment_messages_unviewed_count(chat.id, user.id)
    assert old_item['messagesUnviewedCount'] == 1
    user_manager.sync_chats_with_unviewed_messages_count(chat.id, new_item={}, old_item=old_item)
    assert user.refresh_item().item.get('chatsWithUnviewedMessagesCount', 0) == 0

    # sync delete of member with some unviewed message count, verify fails softly
    with caplog.at_level(logging.WARNING):
        user_manager.sync_chats_with_unviewed_messages_count(chat.id, new_item={}, old_item=old_item)
    assert len(caplog.records) == 1
    assert 'Failed to decrement' in caplog.records[0].msg
    assert 'chatsWithUnviewedMessagesCount' in caplog.records[0].msg
    assert user.id in caplog.records[0].msg


def test_sync_follow_counts_due_to_follow_status_public_user_lifecycle(
    user_manager, follower_manager, user, user2
):
    # configure change to following
    follower, followed = user, user2
    follow = follower_manager.request_to_follow(follower, followed)
    assert follow.status == FollowStatus.FOLLOWING

    # check starting state
    assert follower.refresh_item().item.get('followedCount', 0) == 0
    assert followed.refresh_item().item.get('followerCount', 0) == 0
    assert followed.refresh_item().item.get('followersRequestedCount', 0) == 0

    # sync, check state
    user_manager.sync_follow_counts_due_to_follow_status(followed.id, new_item=follow.item)
    assert follower.refresh_item().item.get('followedCount', 0) == 1
    assert followed.refresh_item().item.get('followerCount', 0) == 1
    assert followed.refresh_item().item.get('followersRequestedCount', 0) == 0

    # sync unfollowing, check state
    user_manager.sync_follow_counts_due_to_follow_status(followed.id, old_item=follow.item)
    assert follower.refresh_item().item.get('followedCount', 0) == 0
    assert followed.refresh_item().item.get('followerCount', 0) == 0
    assert followed.refresh_item().item.get('followersRequestedCount', 0) == 0


def test_sync_follow_counts_due_to_follow_status_private_user_lifecycle(
    user_manager, follower_manager, user, user2
):
    # configure change to requested
    follower, followed = user, user2
    followed.set_privacy_status(UserPrivacyStatus.PRIVATE)
    follow = follower_manager.request_to_follow(follower, followed)
    assert follow.status == FollowStatus.REQUESTED

    # check starting state
    assert follower.refresh_item().item.get('followedCount', 0) == 0
    assert followed.refresh_item().item.get('followerCount', 0) == 0
    assert followed.refresh_item().item.get('followersRequestedCount', 0) == 0

    # sync requested, check state
    user_manager.sync_follow_counts_due_to_follow_status(followed.id, new_item=follow.item)
    assert follower.refresh_item().item.get('followedCount', 0) == 0
    assert followed.refresh_item().item.get('followerCount', 0) == 0
    assert followed.refresh_item().item.get('followersRequestedCount', 0) == 1

    # sync following, check state
    old_item = follow.item.copy()
    follow.accept()
    assert follow.status == FollowStatus.FOLLOWING
    user_manager.sync_follow_counts_due_to_follow_status(followed.id, new_item=follow.item, old_item=old_item)
    assert follower.refresh_item().item.get('followedCount', 0) == 1
    assert followed.refresh_item().item.get('followerCount', 0) == 1
    assert followed.refresh_item().item.get('followersRequestedCount', 0) == 0

    # sync denied, check state
    old_item = follow.item.copy()
    follow.deny()
    assert follow.status == FollowStatus.DENIED
    user_manager.sync_follow_counts_due_to_follow_status(followed.id, new_item=follow.item, old_item=old_item)
    assert follower.refresh_item().item.get('followedCount', 0) == 0
    assert followed.refresh_item().item.get('followerCount', 0) == 0
    assert followed.refresh_item().item.get('followersRequestedCount', 0) == 0

    # sync back to following, check state
    old_item = follow.item.copy()
    follow.accept()
    assert follow.status == FollowStatus.FOLLOWING
    user_manager.sync_follow_counts_due_to_follow_status(followed.id, new_item=follow.item, old_item=old_item)
    assert follower.refresh_item().item.get('followedCount', 0) == 1
    assert followed.refresh_item().item.get('followerCount', 0) == 1
    assert followed.refresh_item().item.get('followersRequestedCount', 0) == 0

    # sync back to not following, check state
    old_item = follow.item.copy()
    assert follow.status == FollowStatus.FOLLOWING
    user_manager.sync_follow_counts_due_to_follow_status(followed.id, old_item=old_item)
    assert follower.refresh_item().item.get('followedCount', 0) == 0
    assert followed.refresh_item().item.get('followerCount', 0) == 0
    assert followed.refresh_item().item.get('followersRequestedCount', 0) == 0


def test_sync_follow_counts_due_to_follow_status_fails_softly(
    user_manager, follower_manager, user, user2, caplog
):
    # configure change from requested -> not following
    follower, followed = user, user2
    followed.set_privacy_status(UserPrivacyStatus.PRIVATE)
    follow = follower_manager.request_to_follow(follower, followed)
    assert follow.status == FollowStatus.REQUESTED

    # check starting state
    assert follower.refresh_item().item.get('followedCount', 0) == 0
    assert followed.refresh_item().item.get('followerCount', 0) == 0
    assert followed.refresh_item().item.get('followersRequestedCount', 0) == 0

    # sync a change that fails to decrement, verify fails softly
    with caplog.at_level(logging.WARNING):
        user_manager.sync_follow_counts_due_to_follow_status(followed.id, old_item=follow.item)
    assert len(caplog.records) == 1
    assert 'Failed to decrement' in caplog.records[0].msg
    assert 'followersRequestedCount' in caplog.records[0].msg
    assert followed.id in caplog.records[0].msg

    # configure change from following -> not following
    follow.accept()
    assert follow.status == FollowStatus.FOLLOWING

    # check starting state
    assert follower.refresh_item().item.get('followedCount', 0) == 0
    assert followed.refresh_item().item.get('followerCount', 0) == 0
    assert followed.refresh_item().item.get('followersRequestedCount', 0) == 0

    # sync a change that fails to decrement, verify fails softly
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        user_manager.sync_follow_counts_due_to_follow_status(followed.id, old_item=follow.item)
    assert len(caplog.records) == 2
    follower_records = [rec for rec in caplog.records if 'followerCount' in rec.msg]
    followed_records = [rec for rec in caplog.records if 'followedCount' in rec.msg]
    assert len(follower_records) == 1
    assert len(followed_records) == 1
    assert all(x in followed_records[0].msg for x in ('Failed to decrement', follower.id))
    assert all(x in follower_records[0].msg for x in ('Failed to decrement', followed.id))

    # check final state
    assert follower.refresh_item().item.get('followedCount', 0) == 0
    assert followed.refresh_item().item.get('followerCount', 0) == 0
    assert followed.refresh_item().item.get('followersRequestedCount', 0) == 0


def test_sync_chat_message_creation_count(user_manager, user2, message, system_message):
    # check starting state
    assert user2.refresh_item().item.get('chatMessagesCreationCount', 0) == 0

    # sync a message creation by user2, verify increments
    user_manager.sync_chat_message_creation_count(message.id, new_item=message.item)
    assert user2.refresh_item().item.get('chatMessagesCreationCount', 0) == 1

    # sync a system message creation, verify no error and no increment
    user_manager.sync_chat_message_creation_count(system_message.id, new_item=system_message.item)
    assert user2.refresh_item().item.get('chatMessagesCreationCount', 0) == 1


def test_sync_chat_message_deletion_count(user_manager, user2, message, system_message):
    # check starting state
    assert user2.refresh_item().item.get('chatMessagesDeletionCount', 0) == 0

    # sync a message deletion by user2, verify increments
    user_manager.sync_chat_message_deletion_count(message.id, old_item=message.item)
    assert user2.refresh_item().item.get('chatMessagesDeletionCount', 0) == 1

    # sync a system message deletion, verify no error and no increment
    user_manager.sync_chat_message_deletion_count(system_message.id, old_item=system_message.item)
    assert user2.refresh_item().item.get('chatMessagesDeletionCount', 0) == 1
