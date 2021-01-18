import uuid
from unittest import mock

import pendulum
import pytest

from app.models import CommentManager, LikeManager
from app.models.post.enums import PostStatus, PostType
from app.utils import image_size


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user
user3 = user


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
def completed_post_with_media(post_manager, user, image_data_b64):
    yield post_manager.add_post(user, 'pid3', PostType.IMAGE, image_input={'imageData': image_data_b64})


@pytest.fixture
def post_with_media(post_manager, user):
    yield post_manager.add_post(
        user, 'pid4', PostType.IMAGE, text='t', image_input={'originalMetadata': '{}', 'imageFormat': 'JPEG'}
    )


def test_delete_completed_text_only_post_with_expiration(post_manager, post_with_expiration, user_manager):
    post = post_with_expiration

    # mock out some calls to far-flung other managers
    post.comment_manager = mock.Mock(CommentManager({}))
    post.follower_manager = mock.Mock(post.follower_manager)
    post.like_manager = mock.Mock(LikeManager({}))

    # delete the post
    post.delete()
    assert post.item['postStatus'] == PostStatus.DELETING
    post_item = post.item

    # check the post is no longer in the DB
    post.refresh_item()
    assert post.item is None

    # check calls to mocked out managers
    assert post.comment_manager.mock_calls == [
        mock.call.delete_all_on_post(post.id),
    ]
    assert post.follower_manager.mock_calls == [
        mock.call.refresh_first_story(story_prev=post_item),
    ]
    assert post.like_manager.mock_calls == [
        mock.call.dislike_all_of_post(post.id),
    ]


def test_delete_pending_media_post(post_manager, post_with_media, user_manager):
    post = post_with_media
    assert post.image_item
    assert post_manager.dynamo.get_post(post_with_media.id)
    assert post_manager.original_metadata_dynamo.get(post_with_media.id)

    # mock out some calls to far-flung other managers
    post.comment_manager = mock.Mock(CommentManager({}))
    post.like_manager = mock.Mock(LikeManager({}))
    post.follower_manager = mock.Mock(post.follower_manager)

    # delete the post
    post.delete()
    assert post.item['postStatus'] == PostStatus.DELETING

    # check the db again
    post.refresh_item()
    post.refresh_image_item()

    assert not post.item
    assert not post.image_item
    assert post_manager.original_metadata_dynamo.get(post_with_media.id) is None

    # check calls to mocked out managers
    assert post.comment_manager.mock_calls == [mock.call.delete_all_on_post(post.id)]
    assert post.like_manager.mock_calls == [mock.call.dislike_all_of_post(post.id)]
    assert post.follower_manager.mock_calls == []


def test_delete_completed_media_post(post_manager, completed_post_with_media, user_manager):
    post = completed_post_with_media

    # mock out some calls to far-flung other managers
    post.comment_manager = mock.Mock(CommentManager({}))
    post.like_manager = mock.Mock(LikeManager({}))
    post.follower_manager = mock.Mock(post.follower_manager)

    # delete the post
    post.delete()
    assert post.item['postStatus'] == PostStatus.DELETING

    # check the all the images got deleted
    for size in image_size.JPEGS:
        path = post.get_image_path(size)
        assert post_manager.clients['s3_uploads'].exists(path) is False

    # check the DB again
    post.refresh_item()
    post.refresh_image_item()

    assert not post.item
    assert not post.image_item

    # check calls to mocked out managers
    assert post.comment_manager.mock_calls == [
        mock.call.delete_all_on_post(post.id),
    ]
    assert post.like_manager.mock_calls == [
        mock.call.dislike_all_of_post(post.id),
    ]
    assert post.follower_manager.mock_calls == []


def test_delete_completed_post_in_album(album_manager, post_manager, post_with_album, user_manager):
    post = post_with_album
    album = album_manager.get_album(post.item['albumId'])
    assert post.item['gsiK3PartitionKey'] == f'post/{album.id}'
    assert post.item['gsiK3SortKey'] == 0

    # check our starting point
    assert post.item['postStatus'] == PostStatus.COMPLETED
    album.refresh_item()
    assert album.item.get('rankCount', 0) == 1

    # mock out some calls to far-flung other managers
    post.comment_manager = mock.Mock(CommentManager({}))
    post.like_manager = mock.Mock(LikeManager({}))
    post.follower_manager = mock.Mock(post.follower_manager)

    # delete the post
    post.delete()
    assert post.item['postStatus'] == PostStatus.DELETING
    assert post.item['gsiK3PartitionKey'] == f'post/{album.id}'
    assert post.item['gsiK3SortKey'] == -1

    # check the DB again
    post.refresh_item()
    assert post.item is None

    # check our post count - should have decremented
    album.refresh_item()
    assert album.item.get('rankCount', 0) == 1

    # check calls to mocked out managers
    assert post.comment_manager.mock_calls == [
        mock.call.delete_all_on_post(post.id),
    ]
    assert post.like_manager.mock_calls == [
        mock.call.dislike_all_of_post(post.id),
    ]
    assert post.follower_manager.mock_calls == []


def test_delete_archived_post(completed_post_with_media):
    post = completed_post_with_media
    post.archive()
    assert post.status == PostStatus.ARCHIVED

    # delete the post
    post.delete()
    assert post.status == PostStatus.DELETING


def test_delete_post_deletes_trending(completed_post_with_media):
    post = completed_post_with_media

    # check post starts with a trending_item
    assert post.trending_item
    assert post.refresh_trending_item().trending_item

    # delete the post, verify the trending item has disappeared
    post.delete()
    assert post.trending_item is None
    assert post.refresh_trending_item().trending_item is None
