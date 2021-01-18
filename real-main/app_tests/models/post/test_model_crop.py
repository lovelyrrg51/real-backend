import uuid
from os import path

import pytest

from app.models.post.enums import PostType
from app.models.post.exceptions import PostException
from app.utils import image_size

jpeg_path = path.join(path.dirname(__file__), '..', '..', 'fixtures', 'grant.jpg')
jpeg_height = 320
jpeg_width = 240

heic_path = path.join(path.dirname(__file__), '..', '..', 'fixtures', 'IMG_0265.HEIC')
heic_height = 3024
heic_width = 4032


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def native_jpeg_cached_image(post_manager, user, s3_uploads_client):
    post = post_manager.add_post(user, str(uuid.uuid4()), PostType.IMAGE, image_input={'imageFormat': 'JPEG'})
    s3_path = post.get_image_path(image_size.NATIVE)
    s3_uploads_client.put_object(s3_path, open(jpeg_path, 'rb'), 'image/jpeg')
    yield post.native_jpeg_cache


@pytest.fixture
def native_heic_cached_image(post_manager, user, s3_uploads_client):
    post = post_manager.add_post(user, str(uuid.uuid4()), PostType.IMAGE, image_input={'imageFormat': 'HEIC'})
    s3_path = post.get_image_path(image_size.NATIVE_HEIC)
    s3_uploads_client.put_object(s3_path, open(heic_path, 'rb'), 'image/heic')
    yield post.native_heic_cache


@pytest.mark.parametrize(
    'cached_image, height, width',
    [
        [pytest.lazy_fixture('native_jpeg_cached_image'), jpeg_height, jpeg_width],
        [pytest.lazy_fixture('native_heic_cached_image'), heic_height, heic_width],
    ],
)
def test_cannot_overcrop_height(user, cached_image, height, width):
    crop = {'upperLeft': {'x': 0, 'y': 0}, 'lowerRight': {'x': width, 'y': height + 1}}
    with pytest.raises(PostException, match='not tall enough'):
        cached_image.crop(crop)


@pytest.mark.parametrize(
    'cached_image, height, width',
    [
        [pytest.lazy_fixture('native_jpeg_cached_image'), jpeg_height, jpeg_width],
        [pytest.lazy_fixture('native_heic_cached_image'), heic_height, heic_width],
    ],
)
def test_cannot_overcrop_width(user, cached_image, height, width):
    crop = {'upperLeft': {'x': 0, 'y': 0}, 'lowerRight': {'x': width + 1, 'y': height}}
    with pytest.raises(PostException, match='not wide enough'):
        cached_image.crop(crop)


@pytest.mark.parametrize(
    'cached_image, height, width',
    [
        [pytest.lazy_fixture('native_jpeg_cached_image'), jpeg_height, jpeg_width],
        [pytest.lazy_fixture('native_heic_cached_image'), heic_height, heic_width],
    ],
)
def test_successful_crop_off_nothing(user, cached_image, height, width):
    # crop the image, check the new image dimensions
    crop = {'upperLeft': {'x': 0, 'y': 0}, 'lowerRight': {'x': width, 'y': height}}
    cached_image.crop(crop)
    assert cached_image.is_synced is True
    assert cached_image.readonly_image.size == (width, height)


@pytest.mark.parametrize(
    'cached_image, min_y, max_y, min_x, max_x',
    [
        [pytest.lazy_fixture('native_jpeg_cached_image'), jpeg_height - 1, jpeg_height, 0, 1],
        [pytest.lazy_fixture('native_jpeg_cached_image'), 0, 1, jpeg_width - 1, jpeg_width],
        [pytest.lazy_fixture('native_jpeg_cached_image'), 0, 1, 0, 1],
        [pytest.lazy_fixture('native_heic_cached_image'), heic_height - 1, heic_height, 0, 1],
        [pytest.lazy_fixture('native_heic_cached_image'), 0, 1, heic_width - 1, heic_width],
        [pytest.lazy_fixture('native_heic_cached_image'), 0, 1, 0, 1],
    ],
)
def test_successful_jpeg_crop_to_minimal(user, cached_image, min_y, max_y, min_x, max_x):
    # crop the image, check the new image dims
    crop = {'upperLeft': {'x': min_x, 'y': min_y}, 'lowerRight': {'x': max_x, 'y': max_y}}
    cached_image.crop(crop)
    assert cached_image.is_synced is False
    assert cached_image.readonly_image.size == (1, 1)


def test_jpeg_metadata_preserved_through_crop(user, native_jpeg_cached_image, s3_uploads_client):
    # get the original exif tags
    exif_data = native_jpeg_cached_image.readonly_image.info['exif']  # raw bytes
    assert exif_data

    # crop the image, check the image dimensions have changed, but the exif data has not
    crop = {'upperLeft': {'x': 8, 'y': 8}, 'lowerRight': {'x': 64, 'y': 64}}
    native_jpeg_cached_image.crop(crop)
    assert native_jpeg_cached_image.is_synced is False
    assert native_jpeg_cached_image.readonly_image.size == (56, 56)
    assert native_jpeg_cached_image.readonly_image.info['exif'] == exif_data
