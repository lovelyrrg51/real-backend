import uuid

import pytest

from app.models.post.enums import PostStatus, PostType
from app.utils import image_size


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def pending_video_post(post_manager, user):
    yield post_manager.add_post(user, 'pid-v', PostType.VIDEO)


@pytest.fixture
def processing_video_post(pending_video_post, s3_uploads_client, grant_data):
    post = pending_video_post
    post.item = post.dynamo.set_post_status(post.item, PostStatus.PROCESSING)
    poster_path = post.get_poster_path()
    s3_uploads_client.put_object(poster_path, grant_data, 'image/jpeg')
    yield post


def test_cant_finish_processing_video_upload_various_errors(post_manager, user, pending_video_post):
    text_only_post = post_manager.add_post(user, 'pid-to', PostType.TEXT_ONLY, text='t')
    with pytest.raises(AssertionError, match='VIDEO'):
        text_only_post.finish_processing_video_upload()

    image_post = post_manager.add_post(user, 'pid-i', PostType.IMAGE)
    with pytest.raises(AssertionError, match='VIDEO'):
        image_post.finish_processing_video_upload()

    with pytest.raises(AssertionError, match='PROCESSING'):
        pending_video_post.finish_processing_video_upload()


def test_start_processing_video_upload_success(processing_video_post, s3_uploads_client):
    post = processing_video_post

    # check starting state
    assert post.item['postStatus'] == PostStatus.PROCESSING
    assert s3_uploads_client.exists(post.get_poster_path())
    assert not s3_uploads_client.exists(post.get_image_path(image_size.NATIVE))
    assert not s3_uploads_client.exists(post.get_image_path(image_size.K4))
    assert not s3_uploads_client.exists(post.get_image_path(image_size.P1080))
    assert not s3_uploads_client.exists(post.get_image_path(image_size.P480))
    assert not s3_uploads_client.exists(post.get_image_path(image_size.P64))

    # do the post processing
    post.finish_processing_video_upload()

    # check final state
    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert not s3_uploads_client.exists(post.get_poster_path())
    assert s3_uploads_client.exists(post.get_image_path(image_size.NATIVE))
    assert s3_uploads_client.exists(post.get_image_path(image_size.K4))
    assert s3_uploads_client.exists(post.get_image_path(image_size.P1080))
    assert s3_uploads_client.exists(post.get_image_path(image_size.P480))
    assert s3_uploads_client.exists(post.get_image_path(image_size.P64))
