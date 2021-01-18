from uuid import uuid4

import pendulum
import pytest

from app.models.appstore.dynamo import AppStoreSubDynamo
from app.models.appstore.exceptions import AppStoreSubAlreadyExists


@pytest.fixture
def appstore_sub_dynamo(dynamo_client):
    yield AppStoreSubDynamo(dynamo_client)


def test_add(appstore_sub_dynamo):
    # configure starting state, verify
    original_transaction_id = str(uuid4())
    user_id = str(uuid4())
    status = str(uuid4())
    original_receipt, latest_receipt = str(uuid4()), str(uuid4())
    latest_receipt_info = {'some': 'value'}
    pending_renewal_info = {'bunchOf': 'stuff'}
    now = pendulum.now('utc')
    next_verification_at = now + pendulum.duration(hours=1)
    assert appstore_sub_dynamo.get(original_transaction_id) is None

    # add a new item, verify format
    item = appstore_sub_dynamo.add(
        original_transaction_id,
        user_id,
        status,
        original_receipt,
        latest_receipt,
        latest_receipt_info,
        pending_renewal_info,
        next_verification_at,
        now=now,
    )
    assert appstore_sub_dynamo.get(original_transaction_id) == item
    assert item == {
        'partitionKey': f'appStoreSub/{original_transaction_id}',
        'sortKey': '-',
        'schemaVersion': 0,
        'userId': user_id,
        'status': status,
        'createdAt': now.to_iso8601_string(),
        'lastVerificationAt': now.to_iso8601_string(),
        'originalReceipt': original_receipt,
        'latestReceipt': latest_receipt,
        'latestReceiptInfo': latest_receipt_info,
        'pendingRenewalInfo': pending_renewal_info,
        'gsiA1PartitionKey': f'appStoreSub/{user_id}',
        'gsiA1SortKey': now.to_iso8601_string(),
        'gsiK1PartitionKey': 'appStoreSub',
        'gsiK1SortKey': next_verification_at.to_iso8601_string(),
    }

    # verify can't re-add another item with same original transaction id
    with pytest.raises(AppStoreSubAlreadyExists):
        appstore_sub_dynamo.add(original_transaction_id, '-', '-', '-', '-', '-', '-', pendulum.now('utc'))
    assert appstore_sub_dynamo.get(original_transaction_id) == item


def test_update(appstore_sub_dynamo):
    now, one_hour = pendulum.now('utc'), pendulum.duration(hours=1)
    otid = str(uuid4())

    # verify can't update item that doesn't exist
    with pytest.raises(appstore_sub_dynamo.client.exceptions.ConditionalCheckFailedException):
        appstore_sub_dynamo.update(otid, '-', '-', '-', '-', now, now)

    # add the item, verify fields that will be changed
    item = appstore_sub_dynamo.add(otid, '-', '-', '-', '-', '-', '-', now, now=now)
    assert appstore_sub_dynamo.get(otid) == item
    assert item['status'] == '-'
    assert item['lastVerificationAt'] == now.to_iso8601_string()
    assert item['latestReceipt'] == '-'
    assert item['latestReceiptInfo'] == '-'
    assert item['pendingRenewalInfo'] == '-'
    assert item['gsiK1SortKey'] == now.to_iso8601_string()

    # update that item, verify correct
    new_item = appstore_sub_dynamo.update(otid, 's', 'lr', 'lri', 'pri', now - one_hour, now + one_hour)
    assert appstore_sub_dynamo.get(otid) == new_item
    assert new_item == {
        **item,
        'status': 's',
        'lastVerificationAt': (now - one_hour).to_iso8601_string(),
        'latestReceipt': 'lr',
        'latestReceiptInfo': 'lri',
        'pendingRenewalInfo': 'pri',
        'gsiK1SortKey': (now + one_hour).to_iso8601_string(),
    }


def test_generate_keys_to_reverify(appstore_sub_dynamo):
    # add two items with different next verification timestamps
    now = pendulum.now('utc')
    one_hour = pendulum.duration(hours=1)
    otid1, otid2 = str(uuid4()), str(uuid4())
    item1 = appstore_sub_dynamo.add(otid1, '-', '-', '-', '-', '-', '-', now - one_hour)
    item2 = appstore_sub_dynamo.add(otid2, '-', '-', '-', '-', '-', '-', now + one_hour)
    key1 = {k: item1[k] for k in ('partitionKey', 'sortKey')}
    key2 = {k: item2[k] for k in ('partitionKey', 'sortKey')}
    assert appstore_sub_dynamo.client.get_item(key1) == item1
    assert appstore_sub_dynamo.client.get_item(key2) == item2

    # test generate none, one and two
    assert list(appstore_sub_dynamo.generate_keys_to_reverify(now - 2 * one_hour)) == []
    assert list(appstore_sub_dynamo.generate_keys_to_reverify(now)) == [key1]
    assert list(appstore_sub_dynamo.generate_keys_to_reverify(now + 2 * one_hour)) == [key1, key2]


def test_generate_keys_by_user(appstore_sub_dynamo):
    # add three items by two different users, verify
    otid1, otid2, otid3 = str(uuid4()), str(uuid4()), str(uuid4())
    user_id1, user_id2 = str(uuid4()), str(uuid4())
    item1 = appstore_sub_dynamo.add(otid1, user_id1, '-', '-', '-', '-', '-', pendulum.now('utc'))
    item2 = appstore_sub_dynamo.add(otid2, user_id2, '-', '-', '-', '-', '-', pendulum.now('utc'))
    item3 = appstore_sub_dynamo.add(otid3, user_id2, '-', '-', '-', '-', '-', pendulum.now('utc'))
    key1 = {k: item1[k] for k in ('partitionKey', 'sortKey')}
    key2 = {k: item2[k] for k in ('partitionKey', 'sortKey')}
    key3 = {k: item3[k] for k in ('partitionKey', 'sortKey')}
    assert appstore_sub_dynamo.client.get_item(key1) == item1
    assert appstore_sub_dynamo.client.get_item(key2) == item2
    assert appstore_sub_dynamo.client.get_item(key3) == item3

    # test generate none, one and two
    assert list(appstore_sub_dynamo.generate_keys_by_user(str(uuid4()))) == []
    assert list(appstore_sub_dynamo.generate_keys_by_user(user_id1)) == [key1]
    assert list(appstore_sub_dynamo.generate_keys_by_user(user_id2)) == [key2, key3]
