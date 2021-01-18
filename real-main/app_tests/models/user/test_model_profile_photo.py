import uuid
from unittest import mock

import pytest

from app.models.post.enums import PostType
from app.models.user.exceptions import UserException
from app.utils import image_size


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user


@pytest.fixture
def pending_post(user, post_manager):
    yield post_manager.add_post(user, 'pid-pend', PostType.IMAGE)


@pytest.fixture
def text_post(user, post_manager):
    yield post_manager.add_post(user, 'pid-to', PostType.TEXT_ONLY, text='lore')


@pytest.fixture
def uploaded_post(user, post_manager, image_data_b64):
    yield post_manager.add_post(user, 'post-id', PostType.IMAGE, image_input={'imageData': image_data_b64})


@pytest.fixture
def another_uploaded_post(user, post_manager, grant_data_b64):
    yield post_manager.add_post(user, 'post-id-2', PostType.IMAGE, image_input={'imageData': grant_data_b64})


@pytest.fixture
def another_users_post(user2, post_manager, grant_data_b64):
    yield post_manager.add_post(user2, 'post-oid', PostType.IMAGE, image_input={'imageData': grant_data_b64})


def test_get_photo_path(user, uploaded_post):
    # without photoPostId set
    for size in image_size.JPEGS:
        assert user.get_photo_path(size) is None

    # set it
    user.update_photo(uploaded_post.id)
    assert user.item['photoPostId'] == uploaded_post.id

    # should now return the paths
    for size in image_size.JPEGS:
        path = user.get_photo_path(size)
        assert path is not None
        assert size.name in path
        assert uploaded_post.id in path


def test_get_placeholder_photo_path(user):
    user.placeholder_photos_directory = 'pp-photo-dir'

    # without placeholderPhotoCode set
    for size in image_size.JPEGS:
        assert user.get_placeholder_photo_path(size) is None

    # set it, just in memory but that's enough
    placeholder_photo_code = 'pp-code'
    user.item['placeholderPhotoCode'] = placeholder_photo_code

    # should now return the paths
    for size in image_size.JPEGS:
        path = user.get_placeholder_photo_path(size)
        assert path == f'{user.placeholder_photos_directory}/{placeholder_photo_code}/{size.name}.jpg'


def test_get_photo_url(user, uploaded_post, cloudfront_client):
    user.placeholder_photos_directory = 'pp-photo-dir'
    user.frontend_resources_domain = 'pp-photo-domain'

    # neither set
    for size in image_size.JPEGS:
        assert user.get_photo_url(size) is None

    # placeholder code set
    placeholder_photo_code = 'pp-code'
    user.item['placeholderPhotoCode'] = placeholder_photo_code
    url_root = f'https://{user.frontend_resources_domain}/{user.placeholder_photos_directory}'
    for size in image_size.JPEGS:
        url = user.get_photo_url(size)
        assert url == f'{url_root}/{placeholder_photo_code}/{size.name}.jpg'

    # photo post set
    user.update_photo(uploaded_post.id)
    assert user.item['photoPostId'] == uploaded_post.id

    presigned_url = {}
    cloudfront_client.configure_mock(**{'generate_presigned_url.return_value': presigned_url})
    cloudfront_client.reset_mock()

    for size in image_size.JPEGS:
        url = user.get_photo_url(size)
        assert url is presigned_url
        path = user.get_photo_path(size)
        assert cloudfront_client.mock_calls == [mock.call.generate_presigned_url(path, ['GET', 'HEAD'])]
        cloudfront_client.reset_mock()


def test_set_photo_multiple_times(user, uploaded_post, another_uploaded_post):
    # verify it's not already set
    user.refresh_item()
    assert 'photoPostId' not in user.item

    # set it
    user.update_photo(uploaded_post.id)
    assert user.item['photoPostId'] == uploaded_post.id

    # verify it stuck in the db
    user.refresh_item()
    assert user.item['photoPostId'] == uploaded_post.id

    # check it's in s3
    for size in image_size.JPEGS:
        path = user.get_photo_path(size)
        assert user.s3_uploads_client.exists(path)

    # pull the photo_data we just set up there
    org_bodies = {}
    for size in image_size.JPEGS:
        path = user.get_photo_path(size)
        org_bodies[size] = list(user.s3_uploads_client.get_object_data_stream(path))

    # change it
    user.update_photo(another_uploaded_post.id)
    assert user.item['photoPostId'] == another_uploaded_post.id

    # verify it stuck in the db
    user.refresh_item()
    assert user.item['photoPostId'] == another_uploaded_post.id

    # pull the new photo_data
    for size in image_size.JPEGS:
        path = user.get_photo_path(size)
        new_body = list(user.s3_uploads_client.get_object_data_stream(path))
        assert new_body != org_bodies[size]

    # verify the old images are still there
    # we don't delete them as there may still be un-expired signed urls pointing to the old images
    for size in image_size.JPEGS:
        path = user.get_photo_path(size, photo_post_id=uploaded_post.id)
        assert user.s3_uploads_client.exists(path)


def test_clear_photo_s3_objects(user, uploaded_post, another_uploaded_post):
    # set it
    user.update_photo(uploaded_post.id)
    assert user.item['photoPostId'] == uploaded_post.id

    # change it
    user.update_photo(another_uploaded_post.id)
    assert user.item['photoPostId'] == another_uploaded_post.id

    # verify a bunch of stuff is in S3 now, old and new
    for size in image_size.JPEGS:
        old_path = user.get_photo_path(size, photo_post_id=uploaded_post.id)
        new_path = user.get_photo_path(size, photo_post_id=another_uploaded_post.id)
        assert user.s3_uploads_client.exists(old_path)
        assert user.s3_uploads_client.exists(new_path)

    # clear it all away
    user.clear_photo_s3_objects()

    # verify all profile photos, old and new, were deleted from s3
    for size in image_size.JPEGS:
        old_path = user.get_photo_path(size, photo_post_id=uploaded_post.id)
        new_path = user.get_photo_path(size, photo_post_id=another_uploaded_post.id)
        assert not user.s3_uploads_client.exists(old_path)
        assert not user.s3_uploads_client.exists(new_path)


def test_update_photo_errors(user, pending_post, text_post, another_users_post, uploaded_post):
    # post doesn't exist
    with pytest.raises(UserException, match='not found'):
        user.update_photo('pid-dne')

    # post isn't an image post
    with pytest.raises(UserException, match='does not have type'):
        user.update_photo(text_post.id)

    # post hasn't/didn't reach COMPLETED
    with pytest.raises(UserException, match='does not have status'):
        user.update_photo(pending_post.id)

    # post isn't ours
    with pytest.raises(UserException, match='does not belong to'):
        user.update_photo(another_users_post.id)

    # post hasn't passed verification
    uploaded_post.dynamo.set_is_verified(uploaded_post.id, False)
    with pytest.raises(UserException, match='is not verified'):
        user.update_photo(uploaded_post.id)
