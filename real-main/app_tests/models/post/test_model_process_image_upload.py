import uuid
from unittest import mock

import pendulum
import pytest

from app.models.post.enums import PostStatus, PostType
from app.models.post.exceptions import PostException
from app.utils import image_size


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def text_only_post(post_manager, user):
    yield post_manager.add_post(user, 'pid1', PostType.TEXT_ONLY, text='t')


@pytest.fixture
def pending_post(post_manager, user):
    yield post_manager.add_post(user, 'pid2', PostType.IMAGE, text='t')


@pytest.fixture
def completed_post(post_manager, user, image_data_b64):
    yield post_manager.add_post(user, 'pid3', PostType.IMAGE, image_input={'imageData': image_data_b64})


def test_cant_process_image_upload_various_errors(
    post_manager, user, pending_post, text_only_post, completed_post
):
    with pytest.raises(AssertionError, match='IMAGE'):
        text_only_post.process_image_upload()

    with pytest.raises(AssertionError, match='PENDING'):
        completed_post.process_image_upload()


def test_process_image_upload_exception_partway_thru_no_jpeg(pending_post):
    assert pending_post.item['postStatus'] == PostStatus.PENDING

    with pytest.raises(PostException, match='native.jpg image data not found'):
        pending_post.process_image_upload()
    assert pending_post.item['postStatus'] == PostStatus.PROCESSING
    assert pending_post.refresh_item().item['postStatus'] == PostStatus.PROCESSING


def test_process_image_upload_exception_partway_thru_bad_heic_data(pending_post, s3_uploads_client):
    assert pending_post.item['postStatus'] == PostStatus.PENDING
    pending_post.image_item['imageFormat'] = 'HEIC'
    s3_heic_path = pending_post.get_image_path(image_size.NATIVE_HEIC)
    s3_uploads_client.put_object(s3_heic_path, b'notheicdata', 'image/heic')

    with pytest.raises(PostException, match='Unable to read HEIC'):
        pending_post.process_image_upload()
    assert pending_post.item['postStatus'] == PostStatus.PROCESSING
    assert pending_post.refresh_item().item['postStatus'] == PostStatus.PROCESSING


def test_process_image_upload_success_jpeg(pending_post, s3_uploads_client, grant_data):
    post = pending_post
    assert post.item['postStatus'] == PostStatus.PENDING
    assert 'imageFormat' not in post.image_item

    # put some data in the mocked s3
    native_path = post.get_image_path(image_size.NATIVE)
    s3_uploads_client.put_object(native_path, grant_data, 'image/jpeg')

    # mock out a bunch of methods
    post.native_jpeg_cache.flush = mock.Mock(wraps=post.native_jpeg_cache.flush)
    post.build_image_thumbnails = mock.Mock(wraps=post.build_image_thumbnails)
    post.set_height_and_width = mock.Mock(wraps=post.set_height_and_width)
    post.set_colors = mock.Mock(wraps=post.set_colors)
    post.set_is_verified = mock.Mock(wraps=post.set_is_verified)
    post.set_checksum = mock.Mock(wraps=post.set_checksum)
    post.complete = mock.Mock(wraps=post.complete)

    now = pendulum.now('utc')
    post.process_image_upload(now=now)

    # check the mocks were called correctly
    assert post.native_jpeg_cache.flush.mock_calls == []
    assert post.build_image_thumbnails.mock_calls == [mock.call()]
    assert post.set_height_and_width.mock_calls == [mock.call()]
    assert post.set_colors.mock_calls == [mock.call()]
    assert post.set_is_verified.mock_calls == [mock.call()]
    assert post.set_checksum.mock_calls == [mock.call()]
    assert post.complete.mock_calls == [mock.call(now=now)]

    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert post.refresh_item().item['postStatus'] == PostStatus.COMPLETED


def test_process_image_upload_success_jpeg_with_crop(pending_post, s3_uploads_client, grant_data):
    post = pending_post
    assert post.item['postStatus'] == PostStatus.PENDING
    post.image_item['crop'] = {'upperLeft': {'x': 4, 'y': 2}, 'lowerRight': {'x': 102, 'y': 104}}

    # put some data in the mocked s3
    native_path = post.get_image_path(image_size.NATIVE)
    s3_uploads_client.put_object(native_path, grant_data, 'image/jpeg')

    # mock out a bunch of methods
    post.native_jpeg_cache.flush = mock.Mock(wraps=post.native_jpeg_cache.flush)
    post.build_image_thumbnails = mock.Mock(wraps=post.build_image_thumbnails)
    post.set_height_and_width = mock.Mock(wraps=post.set_height_and_width)
    post.set_colors = mock.Mock(wraps=post.set_colors)
    post.set_is_verified = mock.Mock(wraps=post.set_is_verified)
    post.set_checksum = mock.Mock(wraps=post.set_checksum)
    post.complete = mock.Mock(wraps=post.complete)

    now = pendulum.now('utc')
    post.process_image_upload(now=now)

    # check the mocks were called correctly
    assert post.native_jpeg_cache.flush.mock_calls == [mock.call()]
    assert post.build_image_thumbnails.mock_calls == [mock.call()]
    assert post.set_height_and_width.mock_calls == [mock.call()]
    assert post.set_colors.mock_calls == [mock.call()]
    assert post.set_is_verified.mock_calls == [mock.call()]
    assert post.set_checksum.mock_calls == [mock.call()]
    assert post.complete.mock_calls == [mock.call(now=now)]

    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert post.refresh_item().item['postStatus'] == PostStatus.COMPLETED


def test_process_image_upload_success_heic_with_crop(pending_post, s3_uploads_client, heic_data):
    post = pending_post
    assert post.item['postStatus'] == PostStatus.PENDING
    post.image_item['imageFormat'] = 'HEIC'
    post.image_item['crop'] = {'upperLeft': {'x': 4, 'y': 2}, 'lowerRight': {'x': 102, 'y': 104}}

    # put some data in the mocked s3
    native_path = post.get_image_path(image_size.NATIVE_HEIC)
    s3_uploads_client.put_object(native_path, heic_data, 'image/heic')
    assert s3_uploads_client.exists(native_path)

    # mock out a bunch of methods
    post.native_jpeg_cache.flush = mock.Mock(wraps=post.native_jpeg_cache.flush)
    post.build_image_thumbnails = mock.Mock(wraps=post.build_image_thumbnails)
    post.set_height_and_width = mock.Mock(wraps=post.set_height_and_width)
    post.set_colors = mock.Mock(wraps=post.set_colors)
    post.set_is_verified = mock.Mock(wraps=post.set_is_verified)
    post.set_checksum = mock.Mock(wraps=post.set_checksum)
    post.complete = mock.Mock(wraps=post.complete)

    now = pendulum.now('utc')
    post.process_image_upload(now=now)

    # check the mocks were called correctly
    assert post.native_jpeg_cache.flush.mock_calls == [mock.call()]
    assert post.build_image_thumbnails.mock_calls == [mock.call()]
    assert post.set_height_and_width.mock_calls == [mock.call()]
    assert post.set_colors.mock_calls == [mock.call()]
    assert post.set_is_verified.mock_calls == [mock.call()]
    assert post.set_checksum.mock_calls == [mock.call()]
    assert post.complete.mock_calls == [mock.call(now=now)]

    # check the heic image was deleted because of the crop
    assert not s3_uploads_client.exists(native_path)

    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert post.refresh_item().item['postStatus'] == PostStatus.COMPLETED


def test_process_image_upload_success_heic_with_no_crop(pending_post, s3_uploads_client, heic_data):
    post = pending_post
    assert post.item['postStatus'] == PostStatus.PENDING
    post.image_item['imageFormat'] = 'HEIC'

    # put some data in the mocked s3
    native_path = post.get_image_path(image_size.NATIVE_HEIC)
    s3_uploads_client.put_object(native_path, heic_data, 'image/heic')

    # process, verify
    post.process_image_upload()
    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert post.refresh_item().item['postStatus'] == PostStatus.COMPLETED

    # check the heic image was _not_ deleted because no crop was requested
    assert s3_uploads_client.exists(native_path)


def test_process_image_upload_success_heic_with_noop_crop(pending_post, s3_uploads_client, heic_data, heic_dims):
    post = pending_post
    assert post.item['postStatus'] == PostStatus.PENDING
    post.image_item['imageFormat'] = 'HEIC'
    post.image_item['crop'] = {
        'upperLeft': {'x': 0, 'y': 0},
        'lowerRight': {'x': heic_dims[0], 'y': heic_dims[1]},
    }

    # put some data in the mocked s3
    native_path = post.get_image_path(image_size.NATIVE_HEIC)
    s3_uploads_client.put_object(native_path, heic_data, 'image/heic')

    # process, verify
    post.process_image_upload()
    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert post.refresh_item().item['postStatus'] == PostStatus.COMPLETED

    # check the heic image was _not_ deleted because the crop matched the image dimensions exactly
    assert s3_uploads_client.exists(native_path)
