from unittest.mock import call, patch
from uuid import uuid4

import pytest

from app.models.follower.enums import FollowStatus
from app.models.post.enums import PostStatus, PostType
from app.utils import GqlNotificationType


@pytest.fixture
def user1(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def follower(follower_manager, user1, user2):
    yield follower_manager.request_to_follow(user1, user1)


@pytest.fixture
def post(post_manager, user1):
    yield post_manager.add_post(user1, str(uuid4()), PostType.TEXT_ONLY, text='t')


user2 = user1


def test_on_user_follow_status_change_sync_feed_starts_following(feed_manager, follower, user1, user2):
    assert follower.item['followStatus'] == FollowStatus.FOLLOWING
    with patch.object(feed_manager, 'add_users_posts_to_feed') as add_users_posts_to_feed_mock:
        with patch.object(feed_manager, 'dynamo') as dynamo_mock:
            with patch.object(feed_manager, 'appsync_client') as appsync_client_mock:
                feed_manager.on_user_follow_status_change_sync_feed(user2.id, new_item=follower.item)
    assert add_users_posts_to_feed_mock.mock_calls == [call(user1.id, user2.id)]
    assert dynamo_mock.mock_calls == []
    assert appsync_client_mock.mock_calls == [
        call.fire_notification(user1.id, GqlNotificationType.USER_FEED_CHANGED),
    ]


@pytest.mark.parametrize('status', [None, FollowStatus.REQUESTED, FollowStatus.DENIED])
def test_on_user_follow_status_change_sync_feed_stops_following(feed_manager, follower, user1, user2, status):
    follower.item['followStatus'] = status
    with patch.object(feed_manager, 'add_users_posts_to_feed') as add_users_posts_to_feed_mock:
        with patch.object(feed_manager, 'dynamo') as dynamo_mock:
            with patch.object(feed_manager, 'appsync_client') as appsync_client_mock:
                feed_manager.on_user_follow_status_change_sync_feed(user2.id, new_item=follower.item)
    assert add_users_posts_to_feed_mock.mock_calls == []
    assert dynamo_mock.mock_calls == [call.delete_by_post_owner(user1.id, user2.id)]
    assert appsync_client_mock.mock_calls == [
        call.fire_notification(user1.id, GqlNotificationType.USER_FEED_CHANGED),
    ]


def test_on_post_status_change_sync_feed_post_completed(feed_manager, post):
    assert post.item['postStatus'] == PostStatus.COMPLETED
    user_ids = [str(uuid4()), str(uuid4())]
    with patch.object(feed_manager, 'add_post_to_followers_feeds', return_value=user_ids) as add_post_mock:
        with patch.object(feed_manager, 'dynamo') as dynamo_mock:
            with patch.object(feed_manager, 'appsync_client') as appsync_client_mock:
                feed_manager.on_post_status_change_sync_feed(post.id, new_item=post.item)
    assert add_post_mock.mock_calls == [call(post.user_id, post.item)]
    assert dynamo_mock.mock_calls == []
    assert appsync_client_mock.mock_calls == [
        call.fire_notification(user_ids[0], GqlNotificationType.USER_FEED_CHANGED),
        call.fire_notification(user_ids[1], GqlNotificationType.USER_FEED_CHANGED),
    ]


@pytest.mark.parametrize(
    'status',
    [PostStatus.PENDING, PostStatus.PROCESSING, PostStatus.ERROR, PostStatus.ARCHIVED, PostStatus.DELETING],
)
def test_on_post_status_change_sync_feed_post_uncompleted(feed_manager, post, status):
    old_item = {**post.item, 'postStatus': 'COMPLETED'}
    new_item = {**post.item, 'postStatus': status}
    user_ids = [str(uuid4()), str(uuid4())]
    with patch.object(feed_manager, 'add_post_to_followers_feeds') as add_post_mock:
        with patch.object(feed_manager, 'dynamo', **{'delete_by_post.return_value': user_ids}) as dynamo_mock:
            with patch.object(feed_manager, 'appsync_client') as appsync_client_mock:
                feed_manager.on_post_status_change_sync_feed(post.id, new_item=new_item, old_item=old_item)
    assert add_post_mock.mock_calls == []
    assert dynamo_mock.mock_calls == [call.delete_by_post(post.id)]
    assert appsync_client_mock.mock_calls == [
        call.fire_notification(user_ids[0], GqlNotificationType.USER_FEED_CHANGED),
        call.fire_notification(user_ids[1], GqlNotificationType.USER_FEED_CHANGED),
    ]
