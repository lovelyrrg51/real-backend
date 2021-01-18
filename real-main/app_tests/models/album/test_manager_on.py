from unittest.mock import call, patch
from uuid import uuid4

import pendulum
import pytest

from app.models.post.enums import PostType
from app.utils import image_size


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def album(album_manager, user):
    yield album_manager.add_album(user.id, str(uuid4()), 'album name')


@pytest.fixture
def post(post_manager, user, image_data_b64):
    yield post_manager.add_post(user, str(uuid4()), PostType.IMAGE, image_input={'imageData': image_data_b64})


album1 = album
album2 = album


def test_on_album_delete_delete_album_art(album_manager, post, user, album, image_data_b64):
    # fire for a delete of an album with no art, verify no error
    assert 'artHash' not in album.item
    album_manager.on_album_delete_delete_album_art(album.id, old_item=album.item)

    # add a post with an image to the album to get some art in S3, verify
    post.set_album(album.id)
    album.update_art_if_needed()
    assert 'artHash' in album.refresh_item().item
    art_paths = [album.get_art_image_path(size) for size in image_size.JPEGS]
    for path in art_paths:
        assert album.s3_uploads_client.exists(path) is True

    # fire for delete of that ablum with art, verify art is deleted from S3
    album_manager.on_album_delete_delete_album_art(album.id, old_item=album.item)
    for path in art_paths:
        assert album.s3_uploads_client.exists(path) is False


def test_on_album_add_edit_sync_delete_at(album_manager, user, album):
    assert 'gsiK1PartitionKey' not in album.item
    assert 'gsiK1SortKey' not in album.item

    # fire for newly created album, verify state
    album_manager.on_album_add_edit_sync_delete_at(album.id, new_item=album.item)
    album.refresh_item()
    assert 'gsiK1PartitionKey' in album.item
    assert 'gsiK1SortKey' in album.item

    # fire for a post added to that newly created album, verify state change
    old_item = album.item
    new_item = dict(album.item, postCount=1)
    album_manager.on_album_add_edit_sync_delete_at(album.id, new_item=new_item, old_item=old_item)
    album.refresh_item()
    assert 'gsiK1PartitionKey' not in album.item
    assert 'gsiK1SortKey' not in album.item

    # fire for another post added to album, verify no-op
    old_item = dict(album.item, postCount=1)
    new_item = dict(album.item, postCount=2)
    with patch.object(album_manager, 'dynamo') as mock_dynamo:
        album_manager.on_album_add_edit_sync_delete_at(album.id, new_item=new_item, old_item=old_item)
    assert mock_dynamo.mock_calls == []
    assert 'gsiK1PartitionKey' not in album.item
    assert 'gsiK1SortKey' not in album.item

    # fire for post removed from that newly created album
    old_item = dict(album.item, postCount=1)
    new_item = dict(album.item, postCount=0)
    album_manager.on_album_add_edit_sync_delete_at(album.id, new_item=new_item, old_item=old_item)
    album.refresh_item()
    assert 'gsiK1PartitionKey' in album.item
    assert 'gsiK1SortKey' in album.item

    # fire a no-op with no posts, verify
    old_item = dict(album.item, postCount=0)
    new_item = dict(album.item)
    with patch.object(album_manager, 'dynamo') as mock_dynamo:
        album_manager.on_album_add_edit_sync_delete_at(album.id, new_item=new_item, old_item=old_item)
    assert mock_dynamo.mock_calls == []
    assert 'gsiK1PartitionKey' in album.item
    assert 'gsiK1SortKey' in album.item


def test_on_post_album_change_update_art_if_needed(album_manager, user, album):
    # check for a new album
    with patch.object(album_manager, 'init_album') as init_album_mock:
        album_manager.on_album_posts_last_updated_at_change_update_art_if_needed(album.id, new_item=album.item)
    assert init_album_mock.mock_calls == [call(album.item), call().update_art_if_needed()]

    # check for a changed album
    with patch.object(album_manager, 'init_album') as init_album_mock:
        album_manager.on_album_posts_last_updated_at_change_update_art_if_needed(
            album.id, new_item=album.item, old_item={'un': 'used'}
        )
    assert init_album_mock.mock_calls == [call(album.item), call().update_art_if_needed()]


def test_on_post_album_change_update_counts_and_timestamps(album_manager, user, album1, album2, post):
    # check starting state
    album1.refresh_item()
    assert 'postCount' not in album1.item
    assert 'postsLastUpdatedAt' not in album1.item
    album2.refresh_item()
    assert 'postCount' not in album2.item
    assert 'postsLastUpdatedAt' not in album2.item

    # trigger for adding a new post with album, but not assigning a rank (like a PENDING post)
    new_item = {**post.item, 'albumId': album1.id, 'gsiK3SortKey': -1}
    album_manager.on_post_album_change_update_counts_and_timestamps(post.id, new_item=new_item)
    assert album1.item == album1.refresh_item().item
    assert album2.item == album2.refresh_item().item

    # trigger for changing which album that non-completed post is in
    old_item = new_item
    new_item = {**new_item, 'albumId': album2.id, 'gsiK3SortKey': -1}
    album_manager.on_post_album_change_update_counts_and_timestamps(post.id, new_item=new_item, old_item=old_item)
    assert album1.item == album1.refresh_item().item
    assert album2.item == album2.refresh_item().item

    # trigger for setting a rank in that album
    old_item = new_item
    new_item = {**new_item, 'albumId': album2.id, 'gsiK3SortKey': 0}
    before = pendulum.now('utc')
    album_manager.on_post_album_change_update_counts_and_timestamps(post.id, new_item=new_item, old_item=old_item)
    after = pendulum.now('utc')
    assert album1.item == album1.refresh_item().item
    album2.refresh_item()
    assert album2.item['postCount'] == 1
    assert before < pendulum.parse(album2.item['postsLastUpdatedAt']) < after

    # trigger for changing to a new album, new non negative one rank
    old_item = new_item
    new_item = {**new_item, 'albumId': album1.id, 'gsiK3SortKey': -0.5}
    before = pendulum.now('utc')
    album_manager.on_post_album_change_update_counts_and_timestamps(post.id, new_item=new_item, old_item=old_item)
    after = pendulum.now('utc')
    album1.refresh_item()
    assert album1.item['postCount'] == 1
    assert before < pendulum.parse(album1.item['postsLastUpdatedAt']) < after
    album2.refresh_item()
    assert album2.item['postCount'] == 0
    assert before < pendulum.parse(album2.item['postsLastUpdatedAt']) < after

    # trigger for setting to a rank of -1 in that album
    old_item = new_item
    new_item = {**new_item, 'albumId': album1.id, 'gsiK3SortKey': -1}
    before = pendulum.now('utc')
    album_manager.on_post_album_change_update_counts_and_timestamps(post.id, new_item=new_item, old_item=old_item)
    after = pendulum.now('utc')
    album1.refresh_item()
    assert album1.item['postCount'] == 0
    assert before < pendulum.parse(album1.item['postsLastUpdatedAt']) < after
    assert album2.item == album2.refresh_item().item

    # trigger for changing to a new album, with new non negative one rank
    old_item = new_item
    new_item = {**new_item, 'albumId': album2.id, 'gsiK3SortKey': 0.3}
    before = pendulum.now('utc')
    album_manager.on_post_album_change_update_counts_and_timestamps(post.id, new_item=new_item, old_item=old_item)
    after = pendulum.now('utc')
    assert album1.item == album1.refresh_item().item
    album2.refresh_item()
    assert album2.item['postCount'] == 1
    assert before < pendulum.parse(album2.item['postsLastUpdatedAt']) < after

    # trigger for deleting the post while in that album
    old_item = new_item
    new_item = new_item.copy()
    new_item.pop('albumId')
    new_item.pop('gsiK3SortKey')
    before = pendulum.now('utc')
    album_manager.on_post_album_change_update_counts_and_timestamps(post.id, new_item=new_item, old_item=old_item)
    after = pendulum.now('utc')
    assert album1.item == album1.refresh_item().item
    album2.refresh_item()
    assert album2.item['postCount'] == 0
    assert before < pendulum.parse(album2.item['postsLastUpdatedAt']) < after


def test_on_user_delete_delete_all_by_user(album_manager, user):
    # delete all for a user that has none, verify no error
    album_manager.on_user_delete_delete_all_by_user(user.id, old_item=user.item)

    # add two albums for our user
    album_id_1, album_id_2 = 'aid1', 'aid2'
    album_manager.add_album(user.id, album_id_1, 'album name')
    album_manager.add_album(user.id, album_id_2, 'album name')

    # verify we can see those albums
    album_items = list(album_manager.dynamo.generate_by_user(user.id))
    assert len(album_items) == 2
    assert album_items[0]['albumId'] == album_id_1
    assert album_items[1]['albumId'] == album_id_2

    # delete them all, verify
    album_manager.on_user_delete_delete_all_by_user(user.id, old_item=user.item)
    assert list(album_manager.dynamo.generate_by_user(user.id)) == []
