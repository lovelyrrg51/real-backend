from uuid import uuid4

import pendulum
import pytest

from app.mixins.view.dynamo import ViewDynamo
from app.mixins.view.exceptions import ViewAlreadyExists, ViewDoesNotExist


@pytest.fixture
def view_dynamo(dynamo_client):
    yield ViewDynamo('itype', dynamo_client)


def test_add_and_increment_view(view_dynamo):
    item_id = 'iid'
    user_id = 'uid'
    view_count = 5
    viewed_at = pendulum.now('utc')
    viewed_at_str = viewed_at.to_iso8601_string()

    # verify can't increment view that doesn't exist
    with pytest.raises(ViewDoesNotExist):
        view_dynamo.increment_view_count(item_id, user_id, 1, pendulum.now('utc'))

    # verify the view does not exist
    assert view_dynamo.get_view(item_id, user_id) is None

    # add a new view, verify form is correct
    view = view_dynamo.add_view(item_id, user_id, view_count, viewed_at)
    assert view == {
        'partitionKey': 'itype/iid',
        'sortKey': 'view/uid',
        'schemaVersion': 0,
        'gsiA1PartitionKey': 'itypeView/iid',
        'gsiA1SortKey': viewed_at_str,
        'gsiA2PartitionKey': 'itypeView/uid',
        'gsiA2SortKey': viewed_at_str,
        'viewCount': 5,
        'firstViewedAt': viewed_at_str,
        'lastViewedAt': viewed_at_str,
    }

    # verify can't add another view with same key
    with pytest.raises(ViewAlreadyExists):
        view_dynamo.add_view(item_id, user_id, 1, pendulum.now('utc'))

    # verify a read from the DB has the form we expect
    assert view_dynamo.get_view(item_id, user_id) == view

    # increment the view, verify the new form is correct
    new_viewed_at = pendulum.now('utc')
    view = view_dynamo.increment_view_count(item_id, user_id, view_count, new_viewed_at)
    assert view == {
        'partitionKey': 'itype/iid',
        'sortKey': 'view/uid',
        'schemaVersion': 0,
        'gsiA1PartitionKey': 'itypeView/iid',
        'gsiA1SortKey': viewed_at_str,
        'gsiA2PartitionKey': 'itypeView/uid',
        'gsiA2SortKey': viewed_at_str,
        'viewCount': 10,
        'firstViewedAt': viewed_at_str,
        'lastViewedAt': new_viewed_at.to_iso8601_string(),
    }

    # verify a read from the DB has the form we expect
    assert view_dynamo.get_view(item_id, user_id) == view


def test_generate_keys_by_item_and_generate_keys_by_user(view_dynamo):
    item_id_1, item_id_2 = str(uuid4()), str(uuid4())
    user_id_1, user_id_2 = str(uuid4()), str(uuid4())

    # user1 views both items, user2 views just item2
    view_dynamo.add_view(item_id_1, user_id_1, 1, pendulum.now('utc'))
    view_dynamo.add_view(item_id_2, user_id_1, 1, pendulum.now('utc'))
    view_dynamo.add_view(item_id_2, user_id_2, 1, pendulum.now('utc'))
    vk11 = view_dynamo.key(item_id_1, user_id_1)
    vk12 = view_dynamo.key(item_id_2, user_id_1)
    vk22 = view_dynamo.key(item_id_2, user_id_2)

    # verify generation by item
    assert list(view_dynamo.generate_keys_by_item(str(uuid4()))) == []
    assert list(view_dynamo.generate_keys_by_item(item_id_1)) == [vk11]
    assert list(view_dynamo.generate_keys_by_item(item_id_2)) == sorted([vk22, vk12], key=lambda x: x['sortKey'])

    # verify generation by user
    assert list(view_dynamo.generate_keys_by_user(str(uuid4()))) == []
    assert list(view_dynamo.generate_keys_by_user(user_id_1)) == [vk11, vk12]
    assert list(view_dynamo.generate_keys_by_user(user_id_2)) == [vk22]


def test_delete_view(view_dynamo):
    # add two views, verify
    item_id1, user_id1 = [str(uuid4()), str(uuid4())]
    item_id2, user_id2 = [str(uuid4()), str(uuid4())]
    view_dynamo.add_view(item_id1, user_id1, 1, pendulum.now('utc'))
    view_dynamo.add_view(item_id2, user_id2, 2, pendulum.now('utc'))
    assert view_dynamo.get_view(item_id1, user_id1)
    assert view_dynamo.get_view(item_id2, user_id2)

    # delete one of the views, verify final state
    resp = view_dynamo.delete_view(item_id1, user_id1)
    assert resp
    assert view_dynamo.get_view(item_id1, user_id1) is None
    assert view_dynamo.get_view(item_id2, user_id2)

    # delete a view that doesn't exist, should fail softly
    resp = view_dynamo.delete_view(item_id1, user_id1)
    assert resp is None
