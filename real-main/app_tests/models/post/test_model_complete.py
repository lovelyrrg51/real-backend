import logging
import uuid
from unittest import mock

import pendulum
import pytest

from app.models.post.enums import PostStatus, PostType
from app.models.post.exceptions import PostException
from app.models.user.enums import UserSubscriptionLevel
from app.models.user.exceptions import UserException
from app.utils import image_size


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def post(post_manager, user):
    yield post_manager.add_post(user, 'pid1', PostType.TEXT_ONLY, text='t')


@pytest.fixture
def post_with_media(post_manager, user):
    post = post_manager.add_post(user, 'pid1', PostType.IMAGE, text='t')
    post.dynamo.set_checksum(post.id, post.item['postedAt'], 'checksum1')
    yield post


@pytest.fixture
def post_set_as_user_photo(post_manager, user):
    post = post_manager.add_post(user, 'pid2', PostType.IMAGE, set_as_user_photo=True)
    post.dynamo.set_checksum(post.id, post.item['postedAt'], 'checksum2')
    post.dynamo.set_is_verified(post.id, True)
    yield post


@pytest.fixture
def post_with_media_with_expiration(post_manager, user):
    post = post_manager.add_post(
        user,
        'pid2',
        PostType.IMAGE,
        text='t',
        lifetime_duration=pendulum.duration(hours=1),
    )
    post.dynamo.set_checksum(post.id, post.item['postedAt'], 'checksum2')
    yield post


@pytest.fixture
def post_with_media_with_album(album_manager, post_manager, user):
    album = album_manager.add_album(user.id, 'aid-3', 'album name 3')
    post = post_manager.add_post(user, 'pid3', PostType.IMAGE, text='t', album_id=album.id)
    post.dynamo.set_checksum(post.id, post.item['postedAt'], 'checksum3')
    yield post


def test_complete_error_for_status(post_manager, post):
    # sneak behind the model change the post's status
    post.item = post_manager.dynamo.set_post_status(post.item, PostStatus.COMPLETED)

    with pytest.raises(PostException) as error_info:
        post.complete()
    assert PostStatus.COMPLETED in str(error_info.value)

    # sneak behind the model change the post's status
    post.item = post_manager.dynamo.set_post_status(post.item, PostStatus.ARCHIVED)

    with pytest.raises(PostException) as error_info:
        post.complete()
    assert PostStatus.ARCHIVED in str(error_info.value)

    # sneak behind the model change the post's status
    post.item = post_manager.dynamo.set_post_status(post.item, PostStatus.DELETING)

    with pytest.raises(PostException) as error_info:
        post.complete()
    assert PostStatus.DELETING in str(error_info.value)


def test_complete(post_manager, post_with_media, user_manager, appsync_client):
    post = post_with_media

    # mock out some calls to far-flung other managers
    post.follower_manager = mock.Mock(post.follower_manager)

    # check starting state
    assert post.item['postStatus'] == PostStatus.PENDING
    assert appsync_client.mock_calls == []

    # complete the post, check state
    post.complete()
    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert 'originalPostId' not in post.item

    # check correct calls happened to far-flung other managers
    assert post.follower_manager.mock_calls == []

    # check the subscription was triggered
    assert len(appsync_client.mock_calls) == 1
    assert 'triggerPostNotification' in str(appsync_client.send.call_args.args[0])
    assert appsync_client.send.call_args.args[1]['input']['postId'] == post.id


def test_complete_with_expiration(post_manager, post_with_media_with_expiration, user_manager):
    post = post_with_media_with_expiration

    # mock out some calls to far-flung other managers
    post.follower_manager = mock.Mock(post.follower_manager)

    # check starting state
    assert post.item['postStatus'] == PostStatus.PENDING

    # complete the post, check state
    post.complete()
    assert post.item['postStatus'] == PostStatus.COMPLETED

    # check correct calls happened to far-flung other managers
    assert post.follower_manager.mock_calls == [mock.call.refresh_first_story(story_now=post.item)]


def test_complete_with_album(album_manager, post_manager, post_with_media_with_album, user_manager, image_data):
    post = post_with_media_with_album
    album = album_manager.get_album(post.item['albumId'])
    assert post.item['gsiK3PartitionKey'] == f'post/{album.id}'
    assert post.item['gsiK3SortKey'] == -1

    # put image out in mocked s3 for the post, so album art can be generated
    path = post.get_image_path(image_size.K4)
    post_manager.clients['s3_uploads'].put_object(path, image_data, 'application/octet-stream')

    # mock out some calls to far-flung other managers
    post.follower_manager = mock.Mock(post.follower_manager)

    # check starting state
    assert album.item.get('rankCount', 0) == 0
    assert post.item['postStatus'] == PostStatus.PENDING

    # complete the post, check state
    post.complete()
    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert post.item['gsiK3PartitionKey'] == f'post/{album.id}'
    assert post.item['gsiK3SortKey'] == 0
    album.refresh_item()
    assert album.item.get('rankCount', 0) == 1

    # check correct calls happened to far-flung other managers
    assert post.follower_manager.mock_calls == []


def test_complete_with_album_has_disappeared(album_manager, post_manager, post_with_media_with_album):
    post = post_with_media_with_album
    album_id = post.item['albumId']

    # sneak into dynamo and delete the album, check starting state
    album_manager.dynamo.delete_album(album_id)
    assert album_manager.dynamo.get_album(album_id) is None
    assert post.item['postStatus'] == PostStatus.PENDING

    # complete the post, check state
    post.complete()
    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert 'albumId' not in post.item
    assert album_manager.dynamo.get_album(album_id) is None


def test_complete_with_original_post(post_manager, post_with_media, post_with_media_with_expiration):
    post1, post2 = post_with_media, post_with_media_with_expiration

    # put some native-size media up in the mock s3, same content
    path1 = post1.get_image_path(image_size.NATIVE)
    path2 = post2.get_image_path(image_size.NATIVE)
    post1.s3_uploads_client.put_object(path1, b'anything', 'application/octet-stream')
    post2.s3_uploads_client.put_object(path2, b'anything', 'application/octet-stream')

    # mock out some calls to far-flung other managers
    post1.follower_manager = mock.Mock(post1.follower_manager)
    post2.follower_manager = mock.Mock(post2.follower_manager)

    # complete the post that has the earlier postedAt, should not get an originalPostId
    post1.set_checksum()
    post1.complete()
    assert post1.item['postStatus'] == PostStatus.COMPLETED
    assert 'originalPostId' not in post1.item
    post1.refresh_item()
    assert post1.item['postStatus'] == PostStatus.COMPLETED
    assert 'originalPostId' not in post1.item

    # complete the post with the later postedAt, *should* get an originalPostId
    post2.set_checksum()
    post2.complete()
    assert post2.item['postStatus'] == PostStatus.COMPLETED
    assert post2.item['originalPostId'] == post1.id
    post2.refresh_item()
    assert post2.item['postStatus'] == PostStatus.COMPLETED
    assert post2.item['originalPostId'] == post1.id


def test_complete_with_set_as_user_photo(post_manager, user, post_with_media, post_set_as_user_photo):
    # complete the post without use_as_user_photo, verify user photo change api no called
    post_with_media.user.update_photo = mock.Mock()
    post_with_media.complete()
    assert post_with_media.user.update_photo.mock_calls == []

    # complete the post with use_as_user_photo, verify user photo change api called
    post_set_as_user_photo.user.update_photo = mock.Mock()
    post_set_as_user_photo.complete()
    assert post_set_as_user_photo.user.update_photo.mock_calls == [mock.call(post_set_as_user_photo.id)]


def test_complete_with_set_as_user_photo_handles_exception(post_manager, user, post_set_as_user_photo, caplog):
    # set up mocks
    post_set_as_user_photo.user.update_photo = mock.Mock(side_effect=UserException('nope'))
    post_set_as_user_photo.appsync.trigger_notification = mock.Mock()

    # complete the post with use_as_user_photo with an exception throw from setting the photo, and
    # verify the rest of the post completion completes correctly
    with caplog.at_level(logging.WARNING):
        post_set_as_user_photo.complete()
    assert len(caplog.records) == 1
    assert 'Unable to set user photo' in str(caplog.records[0])

    assert post_set_as_user_photo.user.update_photo.mock_calls == [mock.call(post_set_as_user_photo.id)]
    assert len(post_set_as_user_photo.appsync.trigger_notification.mock_calls) == 1


def test_which_posts_get_free_trending(post_manager, user, image_data_b64, grant_data_b64):
    now = pendulum.now('utc').start_of('day')  # beginning of day to normalize all the trending values
    # verify text-only post gets some free trending
    post = post_manager.add_post(user, str(uuid.uuid4()), PostType.TEXT_ONLY, text='t', now=now)
    assert post.type == PostType.TEXT_ONLY
    assert post.trending_item['gsiA4SortKey'] == 1

    # verify a image post that fails verification and is original gets reduced trending
    post_manager.clients['post_verification'].configure_mock(**{'verify_image.return_value': False})
    post = post_manager.add_post(
        user,
        str(uuid.uuid4()),
        PostType.IMAGE,
        image_input={'imageData': grant_data_b64},
        now=now,
    )
    assert post.is_verified is False
    assert post.original_post_id == post.id
    assert post.trending_item['gsiA4SortKey'] == 0.5

    # verify a image post that passes verification and is original gets free trending
    post_manager.clients['post_verification'].configure_mock(**{'verify_image.return_value': True})
    post = post_manager.add_post(
        user,
        str(uuid.uuid4()),
        PostType.IMAGE,
        image_input={'imageData': image_data_b64},
        now=now,
    )
    assert post.is_verified is True
    assert post.original_post_id == post.id
    assert post.trending_item['gsiA4SortKey'] == 1

    # verify a image post that passes verification but is not original does not get free trending
    post = post_manager.add_post(
        user,
        str(uuid.uuid4()),
        PostType.IMAGE,
        image_input={'imageData': image_data_b64},
        now=now,
    )
    assert post.is_verified is True
    assert post.original_post_id != post.id
    assert post.trending_item is None

    # check that if the user is a subscriber they get 4x the trending
    assert user.grant_subscription_bonus().subscription_level == UserSubscriptionLevel.DIAMOND
    post = post_manager.add_post(user, str(uuid.uuid4()), PostType.TEXT_ONLY, text='t', now=now)
    assert post.trending_item['gsiA4SortKey'] == 4

    # verify the owner of the posts that got free trending did not get any free trending themselves
    assert user.trending_item is None
    assert user.refresh_trending_item().trending_item is None
