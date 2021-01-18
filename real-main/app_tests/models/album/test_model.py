import io
import logging
import uuid
from os import path
from unittest.mock import Mock, patch

import pytest

from app.models.album.exceptions import AlbumException
from app.models.post.enums import PostType
from app.utils import image_size

grant_horz_path = path.join(path.dirname(__file__), '..', '..', 'fixtures', 'grant-horizontal.jpg')
grant_vert_path = path.join(path.dirname(__file__), '..', '..', 'fixtures', 'grant-vertical.jpg')


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def album(album_manager, user):
    yield album_manager.add_album(user.id, 'aid', 'album name')


@pytest.fixture
def completed_image_post(post_manager, user, image_data_b64, mock_post_verification_api):
    yield post_manager.add_post(user, 'pid1', image_input={'imageData': image_data_b64})


@pytest.fixture
def text_only_post(post_manager, user):
    yield post_manager.add_post(user, 'pid1', PostType.TEXT_ONLY, text='t')


def test_serialize(user, album):
    resp = album.serialize('caller-uid')
    assert resp.pop('ownedBy')['userId'] == user.id
    assert resp == album.item


def test_update(album):
    # check starting state
    assert album.item['name'] == 'album name'
    assert 'description' not in album.item

    # edit both
    album.update(name='new name', description='new desc')
    assert album.item['name'] == 'new name'
    assert album.item['description'] == 'new desc'

    # remove the description
    album.update(description='')
    assert album.item['name'] == 'new name'
    assert 'description' not in album.item

    # check can't delete name
    with pytest.raises(AlbumException):
        album.update(name='')


def test_delete_no_posts(user, album):
    # verify the album really exists
    assert album.refresh_item().item

    # delete, verify
    album.delete()
    assert album.refresh_item().item is None


def test_delete(user, album):
    assert album.refresh_item().item
    album.delete()
    assert album.refresh_item().item is None


def test_get_art_image_path(album):
    # test when album has no art
    assert 'artHash' not in album.item
    for size in image_size.JPEGS:
        assert album.get_art_image_path(size) is None

    # set an artHash, in mem is enough
    album.item['artHash'] = 'deadbeef'
    for size in image_size.JPEGS:
        path = album.get_art_image_path(size)
        assert album.item['ownedByUserId'] in path
        assert 'album' in path
        assert album.id in path
        assert album.item['artHash'] in path
        assert size.name in path
        assert path.startswith(album.get_art_image_path_prefix())


def test_get_art_image_url(album):
    image_url = 'https://the-image.com'
    album.cloudfront_client.configure_mock(**{'generate_presigned_url.return_value': image_url})

    # should get placeholder image when album has no artHash
    assert 'artHash' not in album.item
    domain = 'here.there.com'
    album.frontend_resources_domain = domain
    for size in image_size.JPEGS:
        url = album.get_art_image_url(size)
        assert domain in url
        assert size.name in url

    # set an artHash, in mem is enough
    album.item['artHash'] = 'deadbeef'
    url = album.get_art_image_url(image_size.NATIVE)
    for size in image_size.JPEGS:
        assert album.get_art_image_url(size) == image_url


def test_delete_art_images(album):
    # set an art hash and put imagery in mocked s3
    art_hash = 'hashing'
    for size in image_size.JPEGS:
        media1_path = album.get_art_image_path(size, art_hash)
        album.s3_uploads_client.put_object(media1_path, b'anything', 'application/octet-stream')

    # verify we can see that album art
    for size in image_size.JPEGS:
        path = album.get_art_image_path(size, art_hash)
        assert album.s3_uploads_client.exists(path)

    # delete the art
    album.delete_art_images(art_hash)

    # verify we cannot see that album art anymore
    for size in image_size.JPEGS:
        path = album.get_art_image_path(size, art_hash)
        assert not album.s3_uploads_client.exists(path)


def test_save_art_images(album):
    assert 'artHash' not in album.item
    art_hash = 'the hash'

    # check nothing in S3
    for size in image_size.JPEGS:
        path = album.get_art_image_path(size, art_hash)
        assert not album.s3_uploads_client.exists(path)

    # save an image as the art
    with open(grant_horz_path, 'rb') as fh:
        image_data = fh.read()
    album.save_art_images(art_hash, io.BytesIO(image_data))

    # check all sizes are in S3
    for size in image_size.JPEGS:
        path = album.get_art_image_path(size, art_hash)
        assert album.s3_uploads_client.exists(path)

    # check the value of the native image
    native_path = album.get_art_image_path(image_size.NATIVE, art_hash)
    assert album.s3_uploads_client.get_object_data_stream(native_path).read() == image_data

    # save an new image as the art
    with open(grant_vert_path, 'rb') as fh:
        image_data = fh.read()
    album.save_art_images(art_hash, io.BytesIO(image_data))

    # check all sizes are in S3
    for size in image_size.JPEGS:
        path = album.get_art_image_path(size, art_hash)
        assert album.s3_uploads_client.exists(path)

    # check the value of the native image
    native_path = album.get_art_image_path(image_size.NATIVE, art_hash)
    assert album.s3_uploads_client.get_object_data_stream(native_path).read() == image_data


def test_increment_rank_count(album, caplog):
    assert 'rankCount' not in album.refresh_item().item
    album_id = album.id
    org_album_item = album.item.copy()

    # verify increment from nothing
    assert album.increment_rank_count().id == album.id
    assert {**org_album_item, 'rankCount': 1} == album.item

    # verify increment again
    assert album.increment_rank_count().id == album_id
    assert {**org_album_item, 'rankCount': 2} == album.item

    # verify for album that has disappeared from dynamo
    album.dynamo.delete_album(album_id)
    with caplog.at_level(logging.WARNING):
        assert album.increment_rank_count() is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(x in caplog.records[0].msg for x in ('Failed to increment', 'rankCount', album_id))
    assert album.item is None


def test_rank_count(album):
    with patch.object(album, 'item', None):
        assert album.get_first_rank() is None
        assert album.get_last_rank() is None

    with patch.dict(album.item, {'rankCount': 0}):
        assert album.get_first_rank() is None
        assert album.get_last_rank() is None

    with patch.dict(album.item, {'rankCount': 1}):
        assert album.get_first_rank() == 0
        assert album.get_last_rank() == 0

    with patch.dict(album.item, {'rankCount': 2}):
        assert album.get_first_rank() == pytest.approx(-1 / 3)
        assert album.get_last_rank() == pytest.approx(1 / 3)

    with patch.dict(album.item, {'rankCount': 3}):
        assert album.get_first_rank() == pytest.approx(-2 / 4)
        assert album.get_last_rank() == pytest.approx(2 / 4)

    with patch.dict(album.item, {'rankCount': 4}):
        assert album.get_first_rank() == pytest.approx(-3 / 5)
        assert album.get_last_rank() == pytest.approx(3 / 5)

    with patch.dict(album.item, {'rankCount': 5}):
        assert album.get_first_rank() == pytest.approx(-4 / 6)
        assert album.get_last_rank() == pytest.approx(4 / 6)

    with patch.dict(album.item, {'rankCount': 6}):
        assert album.get_first_rank() == pytest.approx(-5 / 7)
        assert album.get_last_rank() == pytest.approx(5 / 7)


def test_get_post_ids_for_art(album):
    album.post_manager.dynamo.generate_post_ids_in_album = Mock()

    # no post ids
    album.post_manager.dynamo.generate_post_ids_in_album.return_value = []
    assert album.get_post_ids_for_art() == []

    # one post ids
    album.post_manager.dynamo.generate_post_ids_in_album.return_value = list(range(1))
    assert album.get_post_ids_for_art() == [0]

    # three post ids
    album.post_manager.dynamo.generate_post_ids_in_album.return_value = list(range(3))
    assert album.get_post_ids_for_art() == [0]

    # four post ids
    album.post_manager.dynamo.generate_post_ids_in_album.return_value = list(range(4))
    assert album.get_post_ids_for_art() == [0, 1, 2, 3]

    # eigth post ids
    album.post_manager.dynamo.generate_post_ids_in_album.return_value = list(range(8))
    assert album.get_post_ids_for_art() == [0, 1, 2, 3]

    # nine post ids
    album.post_manager.dynamo.generate_post_ids_in_album.return_value = list(range(9))
    assert album.get_post_ids_for_art() == [0, 1, 2, 3, 4, 5, 6, 7, 8]

    # 15 post ids
    album.post_manager.dynamo.generate_post_ids_in_album.return_value = list(range(15))
    assert album.get_post_ids_for_art() == [0, 1, 2, 3, 4, 5, 6, 7, 8]

    # 16 post ids
    album.post_manager.dynamo.generate_post_ids_in_album.return_value = list(range(16))
    assert album.get_post_ids_for_art() == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
