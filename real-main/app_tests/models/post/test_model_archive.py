import uuid
from unittest import mock

import pendulum
import pytest

from app.models import LikeManager
from app.models.post.enums import PostStatus, PostType
from app.models.post.exceptions import PostException


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def post(post_manager, user):
    yield post_manager.add_post(user, 'pid1', PostType.TEXT_ONLY, text='t')


@pytest.fixture
def post_with_expiration(post_manager, user):
    yield post_manager.add_post(
        user,
        'pid2',
        PostType.TEXT_ONLY,
        text='t',
        lifetime_duration=pendulum.duration(hours=1),
    )


@pytest.fixture
def post_with_album(album_manager, post_manager, user, image_data_b64):
    album = album_manager.add_album(user.id, 'aid', 'album name')
    yield post_manager.add_post(
        user,
        'pid2',
        PostType.IMAGE,
        image_input={'imageData': image_data_b64},
        album_id=album.id,
    )


@pytest.fixture
def post_with_media(post_manager, user):
    yield post_manager.add_post(user, 'pid2', text='t')


def test_archive_post_wrong_status(post_manager, post):
    # change the post to DELETING status
    post.item = post_manager.dynamo.set_post_status(post.item, PostStatus.DELETING)

    # verify we can't archive a post if we're in the process of deleting it
    with pytest.raises(PostException):
        post.archive()


def test_archive_expired_completed_post(post_manager, post_with_expiration, user_manager):
    post = post_with_expiration
    posted_by_user_id = post.item['postedByUserId']
    posted_by_user = user_manager.get_user(posted_by_user_id)

    # check our starting post count
    posted_by_user.refresh_item()
    assert posted_by_user.item.get('postForcedArchivingCount', 0) == 0

    # mock out some calls to far-flung other managers
    post.like_manager = mock.Mock(LikeManager({}))
    post.follower_manager = mock.Mock(post.follower_manager)

    # archive the post
    post.archive()
    assert post.item['postStatus'] == PostStatus.ARCHIVED

    # check the post count decremented
    posted_by_user.refresh_item()
    assert posted_by_user.item.get('postForcedArchivingCount', 0) == 0

    # check calls to mocked out managers
    assert post.like_manager.mock_calls == [
        mock.call.dislike_all_of_post(post.id),
    ]
    assert post.follower_manager.mock_calls == [
        mock.call.refresh_first_story(story_prev=post.item),
    ]


def test_archive_completed_post_with_album(album_manager, post_manager, post_with_album, user_manager):
    post = post_with_album
    album = album_manager.get_album(post.item['albumId'])
    assert post.item['gsiK3PartitionKey'] == f'post/{album.id}'
    assert post.item['gsiK3SortKey'] == 0

    # check our starting rank count
    album.refresh_item()
    assert album.item['rankCount'] == 1

    # mock out some calls to far-flung other managers
    post.like_manager = mock.Mock(LikeManager({}))
    post.follower_manager = mock.Mock(post.follower_manager)

    # archive the post
    post.archive()
    assert post.item['postStatus'] == PostStatus.ARCHIVED

    # check the post is still in the album, but since it's no longer completed, it doesn't show in the count
    assert post.item['albumId'] == album.id
    assert post.item['gsiK3PartitionKey'] == f'post/{album.id}'
    assert post.item['gsiK3SortKey'] == -1
    album.refresh_item()
    assert album.item['rankCount'] == 1

    # check calls to mocked out managers
    assert post.like_manager.mock_calls == [
        mock.call.dislike_all_of_post(post.id),
    ]
    assert post.follower_manager.mock_calls == []


def test_forced_archive(post, caplog):
    # check starting state
    assert post.status == PostStatus.COMPLETED
    assert post.user.item.get('postForcedArchivingCount', 0) == 0

    # archive
    post.archive(forced=True)
    post.user.refresh_item()

    # check final state
    assert post.status == PostStatus.ARCHIVED
    assert post.user.item.get('postForcedArchivingCount', 0) == 1


def test_archive_deletes_trending(post):
    # check post starts with a trending_item
    assert post.trending_item
    assert post.refresh_trending_item().trending_item

    # archive the post, verify the trending item has disappeared
    post.archive()
    assert post.trending_item is None
    assert post.refresh_trending_item().trending_item is None
