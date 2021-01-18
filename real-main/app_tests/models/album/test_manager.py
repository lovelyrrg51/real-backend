from uuid import uuid4

import pendulum
import pytest

from app.models.album.exceptions import AlbumException


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def album(album_manager, user):
    yield album_manager.add_album(user.id, str(uuid4()), 'album name')


def test_add_album_minimal(album_manager, user):
    album_id = 'aid'

    # add the album
    before = pendulum.now('utc')
    album = album_manager.add_album(user.id, album_id, 'album name')
    after = pendulum.now('utc')

    # verify the album looks correct
    assert album.id == album_id
    assert album.item['ownedByUserId'] == user.id
    assert album.item['name'] == 'album name'
    assert 'description' not in album.item
    assert album.item['createdAt'] >= before.to_iso8601_string()
    assert album.item['createdAt'] <= after.to_iso8601_string()
    assert album.item.get('postCount', 0) == 0
    assert 'postsLastUpdatedAt' not in album.item


def test_add_album_empty_string_description(album_manager, user):
    # add the album
    album_id = 'aid'
    album = album_manager.add_album(user.id, album_id, 'album name', description='')

    # verify the album looks correct
    assert album.id == album_id
    assert 'description' not in album.item


def test_add_album_maximal(album_manager, user):
    album_id = 'aid'

    # add the album
    now = pendulum.now('utc')
    album = album_manager.add_album(user.id, album_id, 'album name', description='a desc', now=now)

    # verify the album looks correct
    assert album.id == album_id
    assert album.item['ownedByUserId'] == user.id
    assert album.item['name'] == 'album name'
    assert album.item['description'] == 'a desc'
    assert album.item['createdAt'] == now.to_iso8601_string()
    assert album.item.get('postCount', 0) == 0
    assert 'postsLastUpdatedAt' not in album.item


def test_add_album_errors(album_manager, user):
    album_id = 'aid'

    # already used the album_id
    album_manager.add_album(user.id, album_id, 'album name')
    with pytest.raises(AlbumException):
        album_manager.add_album(user.id, album_id, 'album name')


def test_garbage_collect(album_manager, user):
    # add three albums: one not in the index, and three in with diff. deleteAt timestamps
    album1 = album_manager.add_album(user.id, str(uuid4()), 'album name')
    album2 = album_manager.add_album(user.id, str(uuid4()), 'album name')
    album3 = album_manager.add_album(user.id, str(uuid4()), 'album name')
    album4 = album_manager.add_album(user.id, str(uuid4()), 'album name')
    album_manager.dynamo.set_delete_at(album2.id, pendulum.now('utc'))
    album_manager.dynamo.set_delete_at(album3.id, pendulum.now('utc'))
    cutoff1 = pendulum.now('utc')
    album_manager.dynamo.set_delete_at(album4.id, pendulum.now('utc'))

    # verify starting state
    assert album1.refresh_item().item
    assert album2.refresh_item().item
    assert album3.refresh_item().item
    assert album4.refresh_item().item

    # garbage collect for the first cutoff, verify
    assert album_manager.garbage_collect(now=cutoff1) == 2
    assert album1.refresh_item().item
    assert album2.refresh_item().item is None
    assert album3.refresh_item().item is None
    assert album4.refresh_item().item

    # garbage collect for the second cutoff, verify
    assert album_manager.garbage_collect() == 1
    assert album1.refresh_item().item
    assert album2.refresh_item().item is None
    assert album3.refresh_item().item is None
    assert album4.refresh_item().item is None

    # garbage collect with a no-op, verify
    assert album_manager.garbage_collect() == 0
    assert album1.refresh_item().item
    assert album2.refresh_item().item is None
    assert album3.refresh_item().item is None
    assert album4.refresh_item().item is None
