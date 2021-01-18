import base64
import uuid
from decimal import Decimal
from os import path

import pytest

from app.models.post.enums import PostType
from app.utils import image_size

# valid jpegs with different aspect ratios
grant_path = path.join(path.dirname(__file__), '..', '..', 'fixtures', 'grant.jpg')
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
def post1(post_manager, user):
    with open(grant_path, 'rb') as fh:
        image_data = base64.b64encode(fh.read())
    yield post_manager.add_post(user, str(uuid.uuid4()), PostType.IMAGE, image_input={'imageData': image_data})


@pytest.fixture
def post2(post_manager, user):
    with open(grant_horz_path, 'rb') as fh:
        image_data = base64.b64encode(fh.read())
    yield post_manager.add_post(user, str(uuid.uuid4()), PostType.IMAGE, image_input={'imageData': image_data})


@pytest.fixture
def post3(post_manager, user):
    with open(grant_vert_path, 'rb') as fh:
        image_data = base64.b64encode(fh.read())
    yield post_manager.add_post(user, str(uuid.uuid4()), PostType.IMAGE, image_input={'imageData': image_data})


@pytest.fixture
def post4(post_manager, user):
    yield post_manager.add_post(user, str(uuid.uuid4()), PostType.TEXT_ONLY, text='lore ipsum')


post5 = post1
post6 = post2
post7 = post3
post8 = post4
post9 = post1
post10 = post2
post11 = post3
post12 = post4
post13 = post1
post14 = post2
post15 = post3
post16 = post4


def test_update_art_if_needed_no_change_no_posts(album):
    assert 'artHash' not in album.item

    # do the update, check nothing changed
    album.update_art_if_needed()
    assert 'artHash' not in album.item

    # double check nothing changed
    album.refresh_item()
    assert 'artHash' not in album.item


def test_update_art_if_needed_add_change_and_remove_one_post(album, post1, s3_uploads_client):
    assert 'artHash' not in album.item
    # without an art hash, can't calculate s3 paths

    # put the post in the album directly in dynamo
    post1.dynamo.set_album_id(post1.item, album.id, album_rank=0)

    # update art
    album.update_art_if_needed()
    art_hash = album.item['artHash']
    assert art_hash

    # check all art sizes are in S3, native image is correct
    for size in image_size.JPEGS:
        path = album.get_art_image_path(size)
        assert album.s3_uploads_client.exists(path)

    # remove the post from the album directly in dynamo
    post1.dynamo.set_album_id(post1.item, None)

    # update art
    album.update_art_if_needed()
    assert 'artHash' not in album.item

    # check all art sizes were removed from S3
    for size in image_size.JPEGS:
        path = album.get_art_image_path(size, art_hash=art_hash)
        assert not album.s3_uploads_client.exists(path)


def test_changing_post_rank_changes_art(album, post1, post2, s3_uploads_client):
    assert 'artHash' not in album.item

    # put the post in the album directly in dynamo
    post1.dynamo.set_album_id(post1.item, album.id, album_rank=Decimal(0.5))

    # update art
    album.update_art_if_needed()
    assert (first_art_hash := album.item['artHash'])
    assert (first_native_path := album.get_art_image_path(image_size.NATIVE))
    assert (first_art_data := s3_uploads_client.get_object_data_stream(first_native_path).read())

    # put the other post in the album directly, ahead of the firs
    post2.dynamo.set_album_id(post2.item, album.id, album_rank=Decimal('0.2'))

    # update art
    album.update_art_if_needed()
    assert (second_art_hash := album.item['artHash'])
    assert second_art_hash != first_art_hash
    assert (second_native_path := album.get_art_image_path(image_size.NATIVE))
    assert second_native_path != first_native_path
    assert (second_art_data := s3_uploads_client.get_object_data_stream(second_native_path).read())
    assert second_art_data != first_art_data

    # now switch order, directly in dynamo
    post1.dynamo.set_album_rank(post1.id, Decimal('0.1'))

    # update art
    album.update_art_if_needed()
    assert (third_art_hash := album.item['artHash'])
    assert third_art_hash == first_art_hash
    assert (third_native_path := album.get_art_image_path(image_size.NATIVE))
    assert third_native_path == first_native_path
    assert (third_art_data := s3_uploads_client.get_object_data_stream(third_native_path).read())
    assert third_art_data == first_art_data

    # check the thumbnails are all in S3, and all the old thumbs have been removed
    for size in image_size.JPEGS:
        path = album.get_art_image_path(size)
        old_path = album.get_art_image_path(size, art_hash=second_art_hash)
        assert s3_uploads_client.exists(path)
        assert not s3_uploads_client.exists(old_path)


def test_1_4_9_16_posts_in_album(
    album,
    post1,
    post2,
    post3,
    post4,
    post5,
    post6,
    post7,
    post8,
    post9,
    post10,
    post11,
    post12,
    post13,
    post14,
    post15,
    post16,
):
    assert 'artHash' not in album.item
    post_dynamo = post1.dynamo

    # put the first post in the album directly in dynamo, update art, check art exists
    post_dynamo.set_album_id(post1.item, album.id, album_rank=0)
    album.update_art_if_needed()
    assert (art_hash_1 := album.item['artHash'])
    assert (native_path_1 := album.get_art_image_path(image_size.NATIVE))
    assert (native_data_1 := album.s3_uploads_client.get_object_data_stream(native_path_1).read())

    # add 2nd & 3rd posts to the album, update art, check it hasn't changed
    post_dynamo.set_album_id(post2.item, album.id, album_rank=Decimal('0.05'))
    post_dynamo.set_album_id(post3.item, album.id, album_rank=Decimal('0.10'))
    album.update_art_if_needed()
    assert album.item['artHash'] == art_hash_1
    assert album.get_art_image_path(image_size.NATIVE) == native_path_1
    assert album.s3_uploads_client.get_object_data_stream(native_path_1).read() == native_data_1

    # add 4th post to the album, update art, check it changed
    post_dynamo.set_album_id(post4.item, album.id, album_rank=Decimal('0.15'))
    album.update_art_if_needed()
    assert (art_hash_4 := album.item['artHash'])
    assert art_hash_4 != art_hash_1
    assert (native_path_4 := album.get_art_image_path(image_size.NATIVE))
    assert native_path_4 != native_path_1
    assert (native_data_4 := album.s3_uploads_client.get_object_data_stream(native_path_4).read())
    assert native_data_4 != native_data_1

    # add 5th thru 8th posts to the album, update_art, check it hasn't changed
    post_dynamo.set_album_id(post5.item, album.id, album_rank=Decimal('0.20'))
    post_dynamo.set_album_id(post6.item, album.id, album_rank=Decimal('0.25'))
    post_dynamo.set_album_id(post6.item, album.id, album_rank=Decimal('0.30'))
    post_dynamo.set_album_id(post7.item, album.id, album_rank=Decimal('0.35'))
    post_dynamo.set_album_id(post8.item, album.id, album_rank=Decimal('0.40'))
    album.update_art_if_needed()
    assert album.item['artHash'] == art_hash_4
    assert album.get_art_image_path(image_size.NATIVE) == native_path_4
    assert album.s3_uploads_client.get_object_data_stream(native_path_4).read() == native_data_4

    # add 9th post to the album, update_art, check it changed
    post_dynamo.set_album_id(post9.item, album.id, album_rank=Decimal('0.45'))
    album.update_art_if_needed()
    assert (art_hash_9 := album.item['artHash'])
    assert art_hash_9 != art_hash_4
    assert (native_path_9 := album.get_art_image_path(image_size.NATIVE))
    assert native_path_9 != native_path_4
    assert (native_data_9 := album.s3_uploads_client.get_object_data_stream(native_path_9).read())
    assert native_data_9 != native_data_4

    # add 10th thru 15th posts to the album, update art, check it didn't change
    post_dynamo.set_album_id(post10.item, album.id, album_rank=Decimal('0.50'))
    post_dynamo.set_album_id(post11.item, album.id, album_rank=Decimal('0.55'))
    post_dynamo.set_album_id(post12.item, album.id, album_rank=Decimal('0.60'))
    post_dynamo.set_album_id(post13.item, album.id, album_rank=Decimal('0.65'))
    post_dynamo.set_album_id(post14.item, album.id, album_rank=Decimal('0.70'))
    post_dynamo.set_album_id(post15.item, album.id, album_rank=Decimal('0.75'))
    album.update_art_if_needed()
    assert album.item['artHash'] == art_hash_9
    assert album.get_art_image_path(image_size.NATIVE) == native_path_9
    assert album.s3_uploads_client.get_object_data_stream(native_path_9).read() == native_data_9

    # add 16th post to the album, update_art, check it changed
    post_dynamo.set_album_id(post16.item, album.id, album_rank=Decimal('0.80'))
    album.update_art_if_needed()
    assert (art_hash_16 := album.item['artHash'])
    assert art_hash_16 != art_hash_9
    assert (native_path_16 := album.get_art_image_path(image_size.NATIVE))
    assert native_path_16 != native_path_9
    assert (native_data_16 := album.s3_uploads_client.get_object_data_stream(native_path_16).read())
    assert native_data_16 != native_data_9
