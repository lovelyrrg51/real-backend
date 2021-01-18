import decimal
import uuid
from unittest import mock

import pendulum
import pytest

from app.models.post.enums import PostStatus, PostType


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


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
def post_with_media(post_manager, user):
    yield post_manager.add_post(user, 'pid2', PostType.IMAGE, text='t')


@pytest.fixture
def post_with_media_completed(post_manager, user, image_data_b64):
    yield post_manager.add_post(user, 'pid2', PostType.IMAGE, image_input={'imageData': image_data_b64}, text='t')


@pytest.fixture
def album(album_manager, user):
    yield album_manager.add_album(user.id, str(uuid.uuid4()), 'album name')


def test_restore_completed_text_only_post_with_expiration(post_manager, post_with_expiration, user_manager):
    post = post_with_expiration

    # archive the post
    post.archive()
    assert post.item['postStatus'] == PostStatus.ARCHIVED

    # mock out some calls to far-flung other managers
    post.follower_manager = mock.Mock(post.follower_manager)

    # restore the post
    post.restore()
    assert post.item['postStatus'] == PostStatus.COMPLETED

    # check the post straight from the db
    post.refresh_item()
    assert post.item['postStatus'] == PostStatus.COMPLETED

    # check calls to mocked out managers
    assert post.follower_manager.mock_calls == [
        mock.call.refresh_first_story(story_now=post.item),
    ]


def test_restore_completed_media_post(post_manager, post_with_media_completed, user_manager):
    post = post_with_media_completed

    # archive the post
    post.archive()
    assert post.item['postStatus'] == PostStatus.ARCHIVED

    # mock out some calls to far-flung other managers
    post.follower_manager = mock.Mock(post.follower_manager)

    # restore the post
    post.restore()
    assert post.item['postStatus'] == PostStatus.COMPLETED

    # check the DB again
    post.refresh_item()
    assert post.item['postStatus'] == PostStatus.COMPLETED

    # check calls to mocked out managers
    assert post.follower_manager.mock_calls == []


def test_restore_completed_post_in_album(post_manager, post_with_media_completed, user_manager, album):
    post = post_with_media_completed
    post.set_album(album.id)

    # archive the post
    post.archive()
    assert post.item['postStatus'] == PostStatus.ARCHIVED
    assert post.item['gsiK3PartitionKey'] == f'post/{album.id}'
    assert post.item['gsiK3SortKey'] == -1

    # check our starting rank count
    album.refresh_item()
    assert album.item.get('rankCount', 0) == 1

    # mock out some calls to far-flung other managers
    post.follower_manager = mock.Mock(post.follower_manager)

    # restore the post
    post.restore()
    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert post.item['albumId'] == album.id
    assert post.item['gsiK3PartitionKey'] == f'post/{album.id}'
    assert post.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(1 / 3))

    # check the post straight from the db
    post.refresh_item()
    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert post.item['albumId'] == album.id
    assert post.item['gsiK3PartitionKey'] == f'post/{album.id}'
    assert post.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(1 / 3))

    # check our rank count - should have incremented
    album.refresh_item()
    assert album.item.get('rankCount', 0) == 2

    # check calls to mocked out managers
    assert post.follower_manager.mock_calls == []


def test_restore_completed_album_has_disappeared(album_manager, post_manager, post_with_media_completed, album):
    # configure starting state
    post = post_with_media_completed
    post.set_album(album.id)
    album_id = post.item['albumId']
    post.archive()

    # sneak into dynamo and delete the album, check starting state
    album_manager.dynamo.delete_album(album_id)
    assert album_manager.dynamo.get_album(album_id) is None
    assert post.item['albumId'] == album_id
    assert post.item['postStatus'] == PostStatus.ARCHIVED

    # complete the post, check state
    post.restore()
    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert 'albumId' not in post.item
    assert album_manager.dynamo.get_album(album_id) is None
