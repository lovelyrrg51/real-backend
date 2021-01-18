import logging
from unittest.mock import call, patch
from uuid import uuid4

import pendulum
import pytest

from app.models.album.dynamo import AlbumDynamo
from app.models.album.exceptions import AlbumAlreadyExists, AlbumDoesNotExist


@pytest.fixture
def album_dynamo(dynamo_client):
    yield AlbumDynamo(dynamo_client)


@pytest.fixture
def album_item(album_dynamo):
    yield album_dynamo.add_album(str(uuid4()), str(uuid4()), 'album name')


def test_add_album_minimal(album_dynamo):
    album_id = 'aid'
    user_id = 'uid'
    name = 'aname'

    # add the album to the DB
    before_str = pendulum.now('utc').to_iso8601_string()
    album_item = album_dynamo.add_album(album_id, user_id, name)
    after_str = pendulum.now('utc').to_iso8601_string()

    # retrieve the album and verify the format is as we expect
    assert album_dynamo.get_album(album_id) == album_item
    created_at_str = album_item['createdAt']
    assert before_str <= created_at_str
    assert after_str >= created_at_str
    assert album_item == {
        'partitionKey': 'album/aid',
        'sortKey': '-',
        'schemaVersion': 0,
        'gsiA1PartitionKey': 'album/uid',
        'gsiA1SortKey': created_at_str,
        'albumId': 'aid',
        'ownedByUserId': 'uid',
        'createdAt': created_at_str,
        'name': 'aname',
    }


def test_add_album_maximal(album_dynamo):
    album_id = 'aid'
    user_id = 'uid'
    name = 'aname'
    description = 'adesc'

    # add the album to the DB
    created_at = pendulum.now('utc')
    album_item = album_dynamo.add_album(album_id, user_id, name, description=description, created_at=created_at)

    # retrieve the album and verify the format is as we expect
    assert album_dynamo.get_album(album_id) == album_item
    created_at_str = created_at.to_iso8601_string()
    assert album_item == {
        'partitionKey': 'album/aid',
        'sortKey': '-',
        'schemaVersion': 0,
        'gsiA1PartitionKey': 'album/uid',
        'gsiA1SortKey': created_at_str,
        'albumId': 'aid',
        'ownedByUserId': 'uid',
        'createdAt': created_at_str,
        'name': 'aname',
        'description': 'adesc',
    }


def test_cant_add_album_same_album_id(album_dynamo, album_item):
    album_id = album_item['albumId']

    # verify we can't add another album with the same id
    with pytest.raises(AlbumAlreadyExists):
        album_dynamo.add_album(album_id, 'uid2', 'n2')


def test_set(album_dynamo, album_item):
    album_id = album_item['albumId']

    # check starting state
    assert album_item['name'] != 'new name'
    assert 'description' not in album_item

    # change just name
    album_item = album_dynamo.set(album_id, name='new name')
    assert album_item['name'] == 'new name'
    assert 'description' not in album_item

    # change both name and description
    album_item = album_dynamo.set(album_id, name='newer name', description='new desc')
    assert album_item['name'] == 'newer name'
    assert album_item['description'] == 'new desc'

    # delete the description
    album_item = album_dynamo.set(album_id, description='')
    assert album_item['name'] == 'newer name'
    assert 'description' not in album_item


def test_set_errors(album_dynamo, album_item):
    album_id = album_item['albumId']

    # try to set paramters on album that doesn't exist
    with pytest.raises(album_dynamo.client.exceptions.ConditionalCheckFailedException):
        album_dynamo.set(album_id + '-dne', name='new name')

    # try to set with no parameters
    with pytest.raises(AssertionError):
        album_dynamo.set(album_id)

    # try to remove name
    with pytest.raises(AssertionError):
        album_dynamo.set(album_id, name='')


def test_cant_delete_album_doesnt_exist(album_dynamo):
    album_id = 'dne-cid'
    with pytest.raises(AlbumDoesNotExist):
        album_dynamo.delete_album(album_id)


def test_delete_album(album_dynamo, album_item):
    album_id = album_item['albumId']

    # verify we can see the album in the DB
    album_item = album_dynamo.get_album(album_id)
    assert album_item['albumId'] == album_id

    # delete the album, verify
    album_dynamo.delete_album(album_id)
    assert album_dynamo.get_album(album_id) is None


def test_increment_decrement_post_count(album_dynamo, album_item, caplog):
    # check starting state
    album_id = album_item['albumId']
    assert 'postCount' not in album_item
    assert 'postsLastUpdatedAt' not in album_item

    # check we can't decrement or increment album that DNE
    album_id_dne = str(uuid4())
    with caplog.at_level(logging.WARNING):
        album_dynamo.decrement_post_count(album_id_dne)
    assert len(caplog.records) == 1
    assert 'Failed to decrement' in caplog.records[0].msg
    assert album_id_dne in caplog.records[0].msg
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        album_dynamo.increment_post_count(album_id_dne)
    assert len(caplog.records) == 1
    assert 'Failed to increment' in caplog.records[0].msg
    assert album_id_dne in caplog.records[0].msg

    # check we can't decrement below zero
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        album_dynamo.decrement_post_count(album_id)
    assert len(caplog.records) == 1
    assert 'Failed to decrement' in caplog.records[0].msg
    assert album_id in caplog.records[0].msg

    # increment specifiying timestamp
    now = pendulum.now('utc')
    new_album_item = album_dynamo.increment_post_count(album_id, now=now)
    assert album_dynamo.get_album(album_id) == new_album_item
    assert new_album_item.pop('postCount') == 1
    assert new_album_item.pop('postsLastUpdatedAt') == now.to_iso8601_string()
    assert new_album_item == album_item

    # increment without specificying timestamp
    before = pendulum.now('utc')
    new_album_item = album_dynamo.increment_post_count(album_id)
    after = pendulum.now('utc')
    assert album_dynamo.get_album(album_id) == new_album_item
    assert new_album_item.pop('postCount') == 2
    assert before < pendulum.parse(new_album_item.pop('postsLastUpdatedAt')) < after
    assert new_album_item == album_item

    # decrement without specifying timestamp
    before = pendulum.now('utc')
    new_album_item = album_dynamo.decrement_post_count(album_id)
    after = pendulum.now('utc')
    assert album_dynamo.get_album(album_id) == new_album_item
    assert new_album_item.pop('postCount') == 1
    assert before < pendulum.parse(new_album_item.pop('postsLastUpdatedAt')) < after
    assert new_album_item == album_item

    # decrement specifying timestamp
    now = pendulum.now('utc')
    new_album_item = album_dynamo.decrement_post_count(album_id, now=now)
    assert album_dynamo.get_album(album_id) == new_album_item
    assert new_album_item.pop('postCount') == 0
    assert new_album_item.pop('postsLastUpdatedAt') == now.to_iso8601_string()
    assert new_album_item == album_item


def test_update_posts_last_updated_at(album_dynamo, album_item, caplog):
    # check starting state
    album_id = album_item['albumId']
    assert 'postsLastUpdatedAt' not in album_item

    # check we can't update album that DNE
    album_id_dne = str(uuid4())
    with caplog.at_level(logging.WARNING):
        album_dynamo.update_posts_last_updated_at(album_id_dne)
    assert len(caplog.records) == 1
    assert 'Failed to update' in caplog.records[0].msg
    assert album_id_dne in caplog.records[0].msg

    # update without specifiying timestamp
    before = pendulum.now('utc')
    new_album_item = album_dynamo.update_posts_last_updated_at(album_id)
    after = pendulum.now('utc')
    assert album_dynamo.get_album(album_id) == new_album_item
    assert before < pendulum.parse(new_album_item.pop('postsLastUpdatedAt')) < after
    assert new_album_item == album_item

    # update specifiying timestamp
    now = pendulum.now('utc')
    new_album_item = album_dynamo.update_posts_last_updated_at(album_id, now=now)
    assert album_dynamo.get_album(album_id) == new_album_item
    assert pendulum.parse(new_album_item.pop('postsLastUpdatedAt')) == now
    assert new_album_item == album_item


def test_generate_by_user(album_dynamo, album_item):
    album_id = album_item['albumId']
    user_id = album_item['ownedByUserId']

    # test generating for a user with no albums
    assert list(album_dynamo.generate_by_user('other-uid')) == []

    # test generate for user with one album
    album_items = list(album_dynamo.generate_by_user(user_id))
    assert len(album_items) == 1
    assert album_items[0]['albumId'] == album_id

    # add another album for that user
    album_id_2 = 'aid-2'
    album_dynamo.add_album(album_id_2, user_id, 'album name')

    # test generate for user with two albums
    album_items = list(album_dynamo.generate_by_user(user_id))
    assert len(album_items) == 2
    assert album_items[0]['albumId'] == album_id
    assert album_items[1]['albumId'] == album_id_2


def test_set_album_hash(album_dynamo, album_item):
    album_id = album_item['albumId']
    assert 'artHash' not in album_id

    # test setting it to some value
    art_hash = 'ahash'
    album_item['artHash'] = art_hash
    assert album_dynamo.set_album_art_hash(album_id, art_hash) == album_item
    assert album_dynamo.get_album(album_id) == album_item

    # test deleting the hash
    del album_item['artHash']
    assert album_dynamo.set_album_art_hash(album_id, None) == album_item
    assert album_dynamo.get_album(album_id) == album_item


def test_set_and_clear_delete_at(album_dynamo, album_item, caplog):
    album_id = album_item['albumId']
    album_id_dne = str(uuid4())

    # verify both methods fail soft for an album that doesn't exist
    with caplog.at_level(logging.WARNING):
        assert album_dynamo.set_delete_at(album_id_dne, pendulum.now('utc')) is None
    assert len(caplog.records) == 1
    assert 'Failed to set deleteAt GSI' in caplog.records[0].msg
    assert album_id_dne in caplog.records[0].msg
    caplog.clear()

    with caplog.at_level(logging.WARNING):
        assert album_dynamo.clear_delete_at(album_id_dne) is None
    assert len(caplog.records) == 1
    assert 'Failed to clear deleteAt GSI' in caplog.records[0].msg
    assert album_id_dne in caplog.records[0].msg
    caplog.clear()

    # verify we can set it
    delete_at = pendulum.now('utc')
    new_item = album_dynamo.set_delete_at(album_id, delete_at)
    assert album_dynamo.get_album(album_id) == new_item
    assert new_item['gsiK1PartitionKey'] == 'album'
    assert pendulum.parse(new_item['gsiK1SortKey']) == delete_at

    # verify we can set it again
    delete_at = pendulum.now('utc')
    new_item = album_dynamo.set_delete_at(album_id, delete_at)
    assert album_dynamo.get_album(album_id) == new_item
    assert new_item['gsiK1PartitionKey'] == 'album'
    assert pendulum.parse(new_item['gsiK1SortKey']) == delete_at

    # verify we can clear it
    new_item = album_dynamo.clear_delete_at(album_id)
    assert album_dynamo.get_album(album_id) == new_item
    assert 'gsiK1PartitionKey' not in new_item
    assert 'gsiK1SortKey' not in new_item

    # verify we can clear it again
    new_item = album_dynamo.clear_delete_at(album_id)
    assert album_dynamo.get_album(album_id) == new_item
    assert 'gsiK1PartitionKey' not in new_item
    assert 'gsiK1SortKey' not in new_item

    # verify we cannot set if for an album that has a non-zero postCount
    album_dynamo.increment_post_count(album_id)
    with caplog.at_level(logging.WARNING):
        assert album_dynamo.set_delete_at(album_id, pendulum.now('utc')) is None
    assert len(caplog.records) == 1
    assert 'Failed to set deleteAt GSI' in caplog.records[0].msg
    assert album_id in caplog.records[0].msg
    caplog.clear()


def test_generate_keys_to_delete(album_dynamo):
    # test generate empty set
    cutoff1 = pendulum.now('utc')
    assert list(album_dynamo.generate_keys_to_delete(cutoff1)) == []

    # add two albums to the index
    album_item1 = album_dynamo.add_album(str(uuid4()), str(uuid4()), 'album name')
    album_dynamo.set_delete_at(album_item1['albumId'], pendulum.now('utc'))
    cutoff2 = pendulum.now('utc')
    album_item2 = album_dynamo.add_album(str(uuid4()), str(uuid4()), 'album name')
    album_dynamo.set_delete_at(album_item2['albumId'], pendulum.now('utc'))
    cutoff3 = pendulum.now('utc')

    # test generation at different cutoffs
    assert list(album_dynamo.generate_keys_to_delete(cutoff1)) == []
    assert list(album_dynamo.generate_keys_to_delete(cutoff2)) == [
        {k: album_item1[k] for k in ('partitionKey', 'sortKey')},
    ]
    assert list(album_dynamo.generate_keys_to_delete(cutoff3)) == [
        {k: album_item1[k] for k in ('partitionKey', 'sortKey')},
        {k: album_item2[k] for k in ('partitionKey', 'sortKey')},
    ]


def test_increment_rank_count(album_dynamo):
    album_id = str(uuid4())
    with patch.object(album_dynamo, 'client') as dynamo_client_mock:
        album_dynamo.increment_rank_count(album_id)
    assert dynamo_client_mock.mock_calls == [call.increment_count(album_dynamo.pk(album_id), 'rankCount')]
