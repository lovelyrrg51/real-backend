from unittest.mock import call, patch
from uuid import uuid4

import pendulum
import pytest

from app.models.follower.enums import FollowStatus
from app.models.follower.exceptions import FollowerAlreadyExists, FollowerException
from app.models.post.enums import PostType
from app.models.user.enums import UserPrivacyStatus, UserStatus
from app.utils import GqlNotificationType


@pytest.fixture
def users(user_manager, cognito_client):
    "Us and them"
    our_user_id, our_username = str(uuid4()), str(uuid4())[:8]
    their_user_id, their_username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(our_user_id, our_username, verified_email=f'{our_username}@real.app')
    cognito_client.create_user_pool_entry(
        their_user_id, their_username, verified_email=f'{their_username}@real.app'
    )
    our_user = user_manager.create_cognito_only_user(our_user_id, our_username)
    their_user = user_manager.create_cognito_only_user(their_user_id, their_username)
    yield (our_user, their_user)


other_users = users


@pytest.fixture
def their_post(follower_manager, users, post_manager):
    "Give them a completed post with an expiration in the next 24 hours"
    post = post_manager.add_post(
        users[1],
        str(uuid4()),
        PostType.TEXT_ONLY,
        lifetime_duration=pendulum.duration(hours=12),
        text='t',
    )
    yield post


@pytest.fixture
def users_private(follower_manager, users):
    "Us and them, they are private"
    our_user, their_user = users
    their_user.set_privacy_status(UserPrivacyStatus.PRIVATE)
    yield (our_user, their_user)


def test_get_follow_status(follower_manager, users):
    our_user, their_user = users
    assert follower_manager.get_follow_status(our_user.id, our_user.id) == 'SELF'
    assert follower_manager.get_follow_status(our_user.id, their_user.id) == 'NOT_FOLLOWING'

    # we follow them, then unfollow them
    follower_manager.request_to_follow(our_user, their_user)
    assert follower_manager.get_follow_status(our_user.id, their_user.id) == 'FOLLOWING'
    follower_manager.get_follow(our_user.id, their_user.id).unfollow()
    assert follower_manager.get_follow_status(our_user.id, their_user.id) == 'NOT_FOLLOWING'

    # we go private
    our_user.set_privacy_status(UserPrivacyStatus.PRIVATE)

    # they go through the follow request process, checking follow status along the way
    assert follower_manager.get_follow_status(their_user.id, our_user.id) == 'NOT_FOLLOWING'
    follower_manager.request_to_follow(their_user, our_user)
    assert follower_manager.get_follow_status(their_user.id, our_user.id) == 'REQUESTED'
    follower_manager.get_follow(their_user.id, our_user.id).deny()
    assert follower_manager.get_follow_status(their_user.id, our_user.id) == 'DENIED'
    follower_manager.get_follow(their_user.id, our_user.id).accept()
    assert follower_manager.get_follow_status(their_user.id, our_user.id) == 'FOLLOWING'
    follower_manager.get_follow(their_user.id, our_user.id).unfollow()
    assert follower_manager.get_follow_status(their_user.id, our_user.id) == 'NOT_FOLLOWING'


def test_request_to_follow_public_user(follower_manager, users):
    our_user, their_user = users

    # check we're not following them
    assert follower_manager.get_follow(our_user.id, their_user.id) is None

    # follow them, double check
    assert follower_manager.request_to_follow(our_user, their_user).status == FollowStatus.FOLLOWING
    assert follower_manager.get_follow(our_user.id, their_user.id).status == FollowStatus.FOLLOWING

    # check the firstStory
    follow = follower_manager.get_follow(our_user.id, their_user.id)
    follower_user_id, followed_user_id = follow.item['followerUserId'], follow.item['followedUserId']
    pk = {
        'partitionKey': f'user/{followed_user_id}',
        'sortKey': f'follower/{follower_user_id}/firstStory',
    }
    ffs = follower_manager.dynamo.client.get_item(pk)
    assert ffs is None


def test_cant_follow_anonymous_user(follower_manager, users):
    # configure them as anonymous (in mem only)
    our_user, their_user = users
    their_user.item['userStatus'] = UserStatus.ANONYMOUS
    assert their_user.status == UserStatus.ANONYMOUS

    # verify we can't follow them
    with pytest.raises(FollowerException, match='with status'):
        follower_manager.request_to_follow(our_user, their_user)
    assert follower_manager.get_follow(our_user.id, their_user.id) is None


def test_request_to_follow_public_user_with_story(follower_manager, users, their_post):
    our_user, their_user = users

    # follow them, double check
    assert follower_manager.request_to_follow(our_user, their_user).status == FollowStatus.FOLLOWING
    assert follower_manager.get_follow(our_user.id, their_user.id).status == FollowStatus.FOLLOWING


def test_request_to_follow_private_user(follower_manager, users):
    our_user, their_user = users

    # set them to private
    their_user.set_privacy_status(UserPrivacyStatus.PRIVATE)

    # request to follow them, double check
    assert follower_manager.request_to_follow(our_user, their_user).status == FollowStatus.REQUESTED
    assert follower_manager.get_follow(our_user.id, their_user.id).status == FollowStatus.REQUESTED

    # check follow counters
    our_user.refresh_item()
    assert our_user.item.get('followerCount', 0) == 0
    assert our_user.item.get('followedCount', 0) == 0

    their_user.refresh_item()
    assert their_user.item.get('followerCount', 0) == 0
    assert their_user.item.get('followedCount', 0) == 0

    # check the firstStory
    follow = follower_manager.get_follow(our_user.id, their_user.id)
    follower_user_id, followed_user_id = follow.item['followerUserId'], follow.item['followedUserId']
    pk = {
        'partitionKey': f'user/{followed_user_id}',
        'sortKey': f'follower/{follower_user_id}/firstStory',
    }
    ffs = follower_manager.dynamo.client.get_item(pk)
    assert ffs is None


def test_request_to_follow_double_follow(follower_manager, users):
    our_user, their_user = users

    # follow them
    assert follower_manager.request_to_follow(our_user, their_user).status == FollowStatus.FOLLOWING

    # try to follow them again
    with pytest.raises(FollowerAlreadyExists):
        follower_manager.request_to_follow(our_user, their_user)


def test_request_to_follow_blocker_blocked_user(follower_manager, users, block_manager):
    our_user, their_user = users

    # they block us
    block_item = block_manager.block(their_user, our_user)
    assert block_item['blockerUserId'] == their_user.id
    assert block_item['blockedUserId'] == our_user.id

    # verify we can't follow them
    with pytest.raises(FollowerException, match='block'):
        follower_manager.request_to_follow(our_user, their_user)

    # verify they can't follow us
    with pytest.raises(FollowerException, match='block'):
        follower_manager.request_to_follow(their_user, our_user)


def test_accept_all_requested_follow_requests(follower_manager, users_private):
    our_user, their_user = users_private

    # request to follow them
    assert follower_manager.request_to_follow(our_user, their_user).status == FollowStatus.REQUESTED

    # accept all REQUESTED the follow request
    follower_manager.accept_all_requested_follow_requests(their_user.id)
    assert follower_manager.get_follow(our_user.id, their_user.id).status == FollowStatus.FOLLOWING

    # nothing should change if we do it again
    follower_manager.accept_all_requested_follow_requests(their_user.id)
    assert follower_manager.get_follow(our_user.id, their_user.id).status == FollowStatus.FOLLOWING

    # deny the follow request
    assert follower_manager.get_follow(our_user.id, their_user.id).deny().status == FollowStatus.DENIED

    # nothing should change if we do it again
    follower_manager.accept_all_requested_follow_requests(their_user.id)
    assert follower_manager.get_follow(our_user.id, their_user.id).status == FollowStatus.DENIED


def test_delete_all_denied_follow_requests(follower_manager, users_private):
    our_user, their_user = users_private

    # request to follow them
    assert follower_manager.request_to_follow(our_user, their_user).status == FollowStatus.REQUESTED

    # delete all the denied follow requests, should not affect
    follower_manager.delete_all_denied_follow_requests(their_user.id)
    assert follower_manager.get_follow(our_user.id, their_user.id).status == FollowStatus.REQUESTED

    # deny the follow request
    assert follower_manager.get_follow(our_user.id, their_user.id).deny().status == FollowStatus.DENIED

    # delete all the denied follow requests, should disappear
    follower_manager.delete_all_denied_follow_requests(their_user.id)
    assert follower_manager.get_follow(our_user.id, their_user.id) is None


def test_generate_follower_user_ids(follower_manager, users, other_users):
    our_user, their_user = users
    other_user = other_users[0]

    # check we have no followers
    uids = list(follower_manager.generate_follower_user_ids(our_user.id))
    assert len(uids) == 0

    # they follow us
    assert follower_manager.request_to_follow(their_user, our_user).status == FollowStatus.FOLLOWING

    # check we have one follower
    uids = list(follower_manager.generate_follower_user_ids(our_user.id))
    assert uids == [their_user.id]

    # other follows us
    our_user.set_privacy_status(UserPrivacyStatus.PRIVATE)
    assert follower_manager.request_to_follow(other_user, our_user).status == FollowStatus.REQUESTED

    # check we have two followers items
    uids = list(follower_manager.generate_follower_user_ids(our_user.id))
    assert sorted(uids) == sorted([their_user.id, other_user.id])

    # check we can filter them down according to status
    uids = list(follower_manager.generate_follower_user_ids(our_user.id, follow_status=FollowStatus.FOLLOWING))
    assert uids == [their_user.id]
    uids = list(follower_manager.generate_follower_user_ids(our_user.id, follow_status=FollowStatus.REQUESTED))
    assert uids == [other_user.id]


def test_generate_followed_user_ids(follower_manager, users, other_users):
    our_user, their_user = users
    other_user = other_users[0]

    # check we have no followeds
    uids = list(follower_manager.generate_followed_user_ids(our_user.id))
    assert len(uids) == 0

    # we follow them
    assert follower_manager.request_to_follow(our_user, their_user).status == FollowStatus.FOLLOWING

    # check we have one followed
    uids = list(follower_manager.generate_followed_user_ids(our_user.id))
    assert uids == [their_user.id]

    # we follow other
    other_user.set_privacy_status(UserPrivacyStatus.PRIVATE)
    assert follower_manager.request_to_follow(our_user, other_user).status == FollowStatus.REQUESTED

    # check we have two followeds
    uids = list(follower_manager.generate_followed_user_ids(our_user.id))
    assert sorted(uids) == sorted([their_user.id, other_user.id])

    # check we can filter them down according to status
    uids = list(follower_manager.generate_followed_user_ids(our_user.id, follow_status=FollowStatus.FOLLOWING))
    assert uids == [their_user.id]
    uids = list(follower_manager.generate_followed_user_ids(our_user.id, follow_status=FollowStatus.REQUESTED))
    assert uids == [other_user.id]


def test_on_first_story_post_id_change_fire_gql_notifications(follower_manager, users, their_post):
    # create a followed first story item to test with
    our_user, their_user = users
    follower_manager.first_story_dynamo.set_all(iter([our_user.id]), their_post.item)
    fs_key = follower_manager.first_story_dynamo.key(their_user.id, our_user.id)
    fs_item = follower_manager.first_story_dynamo.client.get_item(fs_key)

    # trigger for item creation, verify calls
    with patch.object(follower_manager, 'appsync_client') as appsync_client_mock:
        follower_manager.on_first_story_post_id_change_fire_gql_notifications(their_user.id, new_item=fs_item)
    assert appsync_client_mock.mock_calls == [
        call.fire_notification(
            our_user.id,
            GqlNotificationType.USER_FOLLOWED_USERS_WITH_STORIES_CHANGED,
            postId=their_post.id,
            followedUserId=their_user.id,
        )
    ]

    # trigger for item update, verify calls
    new_post_id = str(uuid4())
    new_item = {**fs_item, 'postId': new_post_id}
    with patch.object(follower_manager, 'appsync_client') as appsync_client_mock:
        follower_manager.on_first_story_post_id_change_fire_gql_notifications(
            their_user.id, new_item=new_item, old_item=fs_item
        )
    assert appsync_client_mock.mock_calls == [
        call.fire_notification(
            our_user.id,
            GqlNotificationType.USER_FOLLOWED_USERS_WITH_STORIES_CHANGED,
            postId=new_post_id,
            followedUserId=their_user.id,
        )
    ]

    # trigger for item delete, verify calls
    with patch.object(follower_manager, 'appsync_client') as appsync_client_mock:
        follower_manager.on_first_story_post_id_change_fire_gql_notifications(their_user.id, old_item=fs_item)
    assert appsync_client_mock.mock_calls == [
        call.fire_notification(
            our_user.id,
            GqlNotificationType.USER_FOLLOWED_USERS_WITH_STORIES_CHANGED,
            followedUserId=their_user.id,
        )
    ]


def test_on_user_follow_status_change_sync_first_story(follower_manager, users, their_post):
    # check starting state has no first story item
    us, them = users
    fs_key = follower_manager.first_story_dynamo.key(them.id, us.id)
    assert follower_manager.first_story_dynamo.client.get_item(fs_key) is None

    # fire for us following them, verify cretes first story
    old_item = None
    new_item = follower_manager.dynamo.add_following(us.id, them.id, FollowStatus.FOLLOWING)
    follower_manager.on_user_follow_status_change_sync_first_story(them.id, new_item=new_item, old_item=old_item)
    assert follower_manager.first_story_dynamo.client.get_item(fs_key)['postId'] == their_post.id

    # fire for us unfollowing them or one of us getting deleted, verify deletes first story
    old_item = new_item
    new_item = None
    follower_manager.on_user_follow_status_change_sync_first_story(them.id, new_item=new_item, old_item=old_item)
    assert follower_manager.first_story_dynamo.client.get_item(fs_key) is None

    # fire for us requesting to follow them, verify nothing happens
    new_item = follower_manager.dynamo.update_following_status(old_item, FollowStatus.REQUESTED)
    old_item = None
    follower_manager.on_user_follow_status_change_sync_first_story(them.id, new_item=new_item, old_item=old_item)
    assert follower_manager.first_story_dynamo.client.get_item(fs_key) is None

    # fire for them accepting our request, verify cretes first story
    old_item = new_item
    new_item = follower_manager.dynamo.update_following_status(old_item, FollowStatus.FOLLOWING)
    follower_manager.on_user_follow_status_change_sync_first_story(them.id, new_item=new_item, old_item=old_item)
    assert follower_manager.first_story_dynamo.client.get_item(fs_key)['postId'] == their_post.id

    # fire for them denying our request, verify deletes first story
    old_item = new_item
    new_item = follower_manager.dynamo.update_following_status(old_item, FollowStatus.DENIED)
    follower_manager.on_user_follow_status_change_sync_first_story(them.id, new_item=new_item, old_item=old_item)
    assert follower_manager.first_story_dynamo.client.get_item(fs_key) is None

    # fire for a user delete, verify no error
    old_item = new_item
    new_item = None
    follower_manager.on_user_follow_status_change_sync_first_story(them.id, new_item=new_item, old_item=old_item)
    assert follower_manager.first_story_dynamo.client.get_item(fs_key) is None


def test_on_user_delete_delete_follower_items(follower_manager, users_private):
    our_user, their_user = users_private

    # we request to follow them, they follow us
    assert follower_manager.request_to_follow(our_user, their_user)
    assert follower_manager.request_to_follow(their_user, our_user)
    assert follower_manager.get_follow(our_user.id, their_user.id).status == FollowStatus.REQUESTED
    assert follower_manager.get_follow(their_user.id, our_user.id).status == FollowStatus.FOLLOWING

    # fire handler for a delete of our user, verify follow items are gone
    follower_manager.on_user_delete_delete_follower_items(our_user.id, old_item=our_user.item)
    assert follower_manager.get_follow(our_user.id, their_user.id) is None
    assert follower_manager.get_follow(their_user.id, our_user.id) is None

    # fire handler again, verify idempotent
    follower_manager.on_user_delete_delete_follower_items(our_user.id, old_item=our_user.item)
    assert follower_manager.get_follow(our_user.id, their_user.id) is None
    assert follower_manager.get_follow(their_user.id, our_user.id) is None
