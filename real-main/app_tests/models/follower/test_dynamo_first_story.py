from uuid import uuid4

import pytest

from app.models.follower.dynamo.first_story import FirstStoryDynamo


@pytest.fixture
def fs_dynamo(dynamo_client):
    yield FirstStoryDynamo(dynamo_client)


@pytest.fixture
def story():
    yield {
        'postId': 'pid',
        'postedByUserId': 'pb-uid',
        'expiresAt': 'e-at',
        'postedAt': 'p-at',
    }


def test_key_parse_key(fs_dynamo):
    follower_user_id, followed_user_id = str(uuid4()), str(uuid4())
    key = fs_dynamo.key(followed_user_id, follower_user_id)
    assert len(key) == 2
    assert key['partitionKey'].split('/') == ['user', followed_user_id]
    assert key['sortKey'].split('/') == ['follower', follower_user_id, 'firstStory']
    assert fs_dynamo.parse_key(key) == (followed_user_id, follower_user_id)


def test_set_all_no_followers(fs_dynamo, story):
    fs_dynamo.set_all((uid for uid in []), story)
    # check no items were added to the db
    resp = fs_dynamo.client.table.scan()
    assert resp['Count'] == 0


def test_delete_all_no_followers(fs_dynamo, story):
    fs_dynamo.delete_all((uid for uid in []), story['postedByUserId'])
    # check no items were added to the db
    resp = fs_dynamo.client.table.scan()
    assert resp['Count'] == 0


def test_set_correct_format(fs_dynamo, story):
    fs_dynamo.set_all((uid for uid in ['f-uid']), story)

    # get that one item from the db
    resp = fs_dynamo.client.table.scan()
    assert resp['Count'] == 1
    item = resp['Items'][0]
    assert item == {
        'schemaVersion': 1,
        'partitionKey': 'user/pb-uid',
        'sortKey': 'follower/f-uid/firstStory',
        'gsiA2PartitionKey': 'follower/f-uid/firstStory',
        'gsiA2SortKey': 'e-at',
        'postId': 'pid',
    }


def test_set_all_and_delete_all(fs_dynamo, story):
    # check we start with nothing in DB
    resp = fs_dynamo.client.table.scan()
    assert resp['Count'] == 0

    # put two items in the DB, make sure they got there correctly
    fs_dynamo.set_all((uid for uid in ['f-uid-2', 'f-uid-3']), story)
    resp = fs_dynamo.client.table.scan()
    assert resp['Count'] == 2
    assert all(item['partitionKey'] == 'user/pb-uid' for item in resp['Items'])
    sks = sorted(map(lambda item: item['sortKey'], resp['Items']))
    assert sks == ['follower/f-uid-2/firstStory', 'follower/f-uid-3/firstStory']

    # put another item in the DB, check DB again
    fs_dynamo.set_all((uid for uid in ['f-uid-1']), story)
    resp = fs_dynamo.client.table.scan()
    assert resp['Count'] == 3
    assert all(item['partitionKey'] == 'user/pb-uid' for item in resp['Items'])
    sks = sorted(map(lambda item: item['sortKey'], resp['Items']))
    assert sks == [
        'follower/f-uid-1/firstStory',
        'follower/f-uid-2/firstStory',
        'follower/f-uid-3/firstStory',
    ]

    # delete two items from the DB, check
    fs_dynamo.delete_all((uid for uid in ['f-uid-1', 'f-uid-3']), story['postedByUserId'])
    resp = fs_dynamo.client.table.scan()
    assert resp['Count'] == 1
    assert all(item['partitionKey'] == 'user/pb-uid' for item in resp['Items'])
    sks = sorted(map(lambda item: item['sortKey'], resp['Items']))
    assert sks == ['follower/f-uid-2/firstStory']

    # delete remaing item from the DB, check
    fs_dynamo.delete_all((uid for uid in ['f-uid-2']), story['postedByUserId'])
    resp = fs_dynamo.client.table.scan()
    assert resp['Count'] == 0
