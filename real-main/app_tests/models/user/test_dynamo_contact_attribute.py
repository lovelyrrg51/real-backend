from uuid import uuid4

import pytest

from app.models.user.dynamo import UserContactAttributeDynamo


@pytest.fixture
def uca_dynamo(dynamo_client):
    yield UserContactAttributeDynamo(dynamo_client, 'somePrefix')


def test_basic_add_get_delete(uca_dynamo):
    # check starting state
    attr_value = 'the-value'
    assert uca_dynamo.get(attr_value) is None

    # add it, verify format
    user_id = str(uuid4())
    item = uca_dynamo.add(attr_value, user_id)
    assert uca_dynamo.get(attr_value) == item
    assert item == {
        'partitionKey': f'somePrefix/{attr_value}',
        'sortKey': '-',
        'schemaVersion': 0,
        'userId': user_id,
    }
    assert uca_dynamo.get(attr_value + 'nope') is None

    # check we can't re-add it for another user
    user_id_2 = str(uuid4())
    with pytest.raises(uca_dynamo.client.exceptions.ConditionalCheckFailedException):
        uca_dynamo.add(attr_value, user_id_2)
    assert uca_dynamo.get(attr_value) == item

    # check that user can take their own different value
    attr_value_2 = 'other-value'
    item_2 = uca_dynamo.add(attr_value_2, user_id_2)
    assert uca_dynamo.get(attr_value_2) == item_2
    assert uca_dynamo.get(attr_value) == item

    # delete a value, make sure it goes away
    assert uca_dynamo.delete(attr_value, user_id) == item
    assert uca_dynamo.get(attr_value) is None

    # check deletes are omnipotent
    assert uca_dynamo.delete(attr_value, user_id) is None
    assert uca_dynamo.get(attr_value) is None

    # verify can't delete a value if we supply the wrong user_id
    with pytest.raises(uca_dynamo.client.exceptions.ConditionalCheckFailedException):
        uca_dynamo.delete(attr_value_2, user_id)
    assert uca_dynamo.get(attr_value_2) == item_2


def test_batch_get_user_ids(uca_dynamo):
    # add two items
    user_id_1, user_id_2 = str(uuid4()), str(uuid4())
    attr_1, attr_2 = str(uuid4())[:8], str(uuid4())[:8]
    uca_dynamo.add(attr_1, user_id_1)
    uca_dynamo.add(attr_2, user_id_2)
    assert uca_dynamo.get(attr_1)
    assert uca_dynamo.get(attr_2)

    # check batch get of none
    assert uca_dynamo.batch_get_user_ids([]) == []
    assert uca_dynamo.batch_get_user_ids([str(uuid4())]) == []

    # check batch get of one
    assert uca_dynamo.batch_get_user_ids([attr_1]) == [user_id_1]
    assert uca_dynamo.batch_get_user_ids([str(uuid4()), attr_2]) == [user_id_2]

    # check batch get of two
    assert uca_dynamo.batch_get_user_ids([attr_1, attr_2]).sort() == [user_id_1, user_id_2].sort()
    assert uca_dynamo.batch_get_user_ids([attr_2, attr_1, str(uuid4())]).sort() == [user_id_2, user_id_1].sort()

    # check can handle duplicates
    assert uca_dynamo.batch_get_user_ids([attr_1, attr_1]) == [user_id_1]
    assert uca_dynamo.batch_get_user_ids([attr_1, attr_2, attr_1]).sort() == [user_id_1, user_id_2].sort()


def test_batch_get_user_ids_attr_mapped(uca_dynamo):
    # add two items
    user_id_1, user_id_2 = str(uuid4()), str(uuid4())
    attr_1, attr_2 = str(uuid4())[:8], str(uuid4())[:8]
    uca_dynamo.add(attr_1, user_id_1)
    uca_dynamo.add(attr_2, user_id_2)
    assert uca_dynamo.get(attr_1)
    assert uca_dynamo.get(attr_2)

    # check batch get of none
    assert uca_dynamo.batch_get_user_ids_attr_mapped([]) == {}
    assert uca_dynamo.batch_get_user_ids_attr_mapped([str(uuid4())]) == {}

    # check batch get of one
    assert uca_dynamo.batch_get_user_ids_attr_mapped([attr_1]) == {attr_1: user_id_1}
    assert uca_dynamo.batch_get_user_ids_attr_mapped([str(uuid4()), attr_2]) == {attr_2: user_id_2}

    # check batch get of two
    assert uca_dynamo.batch_get_user_ids_attr_mapped([attr_1, attr_2]) == {
        attr_1: user_id_1,
        attr_2: user_id_2,
    }
    assert uca_dynamo.batch_get_user_ids_attr_mapped([attr_2, attr_1, str(uuid4())]) == {
        attr_2: user_id_2,
        attr_1: user_id_1,
    }

    # check can handle duplicates
    assert uca_dynamo.batch_get_user_ids_attr_mapped([attr_1, attr_1]) == {attr_1: user_id_1}
    assert uca_dynamo.batch_get_user_ids_attr_mapped([attr_1, attr_2, attr_1]) == {
        attr_1: user_id_1,
        attr_2: user_id_2,
    }
