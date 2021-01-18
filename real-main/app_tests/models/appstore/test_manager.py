from unittest.mock import call, patch
from uuid import uuid4

import pendulum
import pytest

from app.models.appstore.enums import AppStoreSubscriptionStatus
from app.models.appstore.exceptions import AppStoreException, AppStoreSubAlreadyExists


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user1 = user
user2 = user


def test_add_receipt(appstore_manager, user):
    # configure two receipts, check starting state
    receipt_data_1, receipt_data_2 = str(uuid4()), str(uuid4())
    original_transaction_id_1, original_transaction_id_2 = str(uuid4()), str(uuid4())
    verify_1 = {
        'latest_receipt': str(uuid4()),
        'latest_receipt_info': {},
        'original_transaction_id': original_transaction_id_1,
        'pending_renewal_info': {},
    }
    verify_2 = {
        'latest_receipt': str(uuid4()),
        'latest_receipt_info': {},
        'original_transaction_id': original_transaction_id_2,
        'pending_renewal_info': {},
    }
    assert appstore_manager.sub_dynamo.get(original_transaction_id_1) is None
    assert appstore_manager.sub_dynamo.get(original_transaction_id_2) is None

    # add one of the receipts, verify
    with patch.object(appstore_manager.appstore_client, 'verify_receipt', return_value=verify_1):
        appstore_manager.add_receipt(receipt_data_1, user.id)
    assert appstore_manager.sub_dynamo.get(original_transaction_id_1)
    assert appstore_manager.sub_dynamo.get(original_transaction_id_2) is None

    # add the other receipt, verify
    with patch.object(appstore_manager.appstore_client, 'verify_receipt', return_value=verify_2):
        appstore_manager.add_receipt(receipt_data_2, user.id)
    assert appstore_manager.sub_dynamo.get(original_transaction_id_1)
    assert appstore_manager.sub_dynamo.get(original_transaction_id_2)

    # verify can't double-add a receipt
    with pytest.raises(AppStoreSubAlreadyExists, match=original_transaction_id_2):
        with patch.object(appstore_manager.appstore_client, 'verify_receipt', return_value=verify_2):
            appstore_manager.add_receipt(receipt_data_2, user.id)


def test_determine_status(appstore_manager):
    determine_status = appstore_manager.determine_status
    now = pendulum.now('utc')
    # apple's API reports datetimes as strings of milliseconds :facepalm:
    before_ts = str(int(now.timestamp() * 1000 - 1))
    after_ts = str(int(now.timestamp() * 1000 + 1))

    assert determine_status({}, now) == AppStoreSubscriptionStatus.ACTIVE
    assert determine_status({'unrelated': '1'}, now) == AppStoreSubscriptionStatus.ACTIVE
    assert determine_status({'cancellation_date_ms': before_ts}, now) == AppStoreSubscriptionStatus.CANCELLED
    assert determine_status({'cancellation_date_ms': after_ts}, now) == AppStoreSubscriptionStatus.ACTIVE
    assert determine_status({'expires_date_ms': before_ts}, now) == AppStoreSubscriptionStatus.EXPIRED
    assert determine_status({'expires_date_ms': after_ts}, now) == AppStoreSubscriptionStatus.ACTIVE
    assert (
        determine_status({'cancellation_date_ms': before_ts, 'expires_date_ms': before_ts}, now)
        == AppStoreSubscriptionStatus.CANCELLED
    )
    assert (
        determine_status({'cancellation_date_ms': after_ts, 'expires_date_ms': after_ts}, now)
        == AppStoreSubscriptionStatus.ACTIVE
    )


def test_on_user_delete_delete_all_by_user(appstore_manager, user1, user2):
    # add subscriptions for the two users directly to dynamo
    otid1, otid2, otid3 = str(uuid4()), str(uuid4()), str(uuid4())
    appstore_manager.sub_dynamo.add(otid1, user2.id, '-', '-', '-', '-', '-', pendulum.now('utc'))
    appstore_manager.sub_dynamo.add(otid2, user1.id, '-', '-', '-', '-', '-', pendulum.now('utc'))
    appstore_manager.sub_dynamo.add(otid3, user2.id, '-', '-', '-', '-', '-', pendulum.now('utc'))
    assert appstore_manager.sub_dynamo.get(otid1)
    assert appstore_manager.sub_dynamo.get(otid2)
    assert appstore_manager.sub_dynamo.get(otid3)

    # fire for unrelated user, verify
    appstore_manager.on_user_delete_delete_all_by_user(str(uuid4()), old_item={})
    assert appstore_manager.sub_dynamo.get(otid1)
    assert appstore_manager.sub_dynamo.get(otid2)
    assert appstore_manager.sub_dynamo.get(otid3)

    # fire for user with two subs, verify
    appstore_manager.on_user_delete_delete_all_by_user(user2.id, old_item=user2.item)
    assert appstore_manager.sub_dynamo.get(otid1) is None
    assert appstore_manager.sub_dynamo.get(otid2)
    assert appstore_manager.sub_dynamo.get(otid3) is None


def test_update_subscriptions(appstore_manager, user1, user2):
    # add subscriptions for the two users directly to dynamo
    now, one_min = pendulum.now('utc'), pendulum.duration(minutes=1)
    otid1, otid2, otid3 = str(uuid4()), str(uuid4()), str(uuid4())
    appstore_manager.sub_dynamo.add(otid1, user2.id, '-', 'or1', '-', '-', '-', now - one_min)
    appstore_manager.sub_dynamo.add(otid2, user1.id, '-', 'or2', '-', '-', '-', now)
    appstore_manager.sub_dynamo.add(otid3, user2.id, '-', 'or3', '-', '-', '-', now + one_min)

    # run with no updates
    with patch.object(appstore_manager, 'update_subscription') as us_mock:
        assert appstore_manager.update_subscriptions(now=(now - 2 * one_min)) == 0
    assert us_mock.mock_calls == []

    # run with two updates
    with patch.object(appstore_manager, 'update_subscription') as us_mock:
        assert appstore_manager.update_subscriptions(now=now) == 2
    assert us_mock.mock_calls == [call(otid1, 'or1'), call(otid2, 'or2')]


def test_update_subscription_different_original_transaction_id(appstore_manager, user):
    # add subscription directly to dynamo
    our_otid, appstore_otid = str(uuid4()), str(uuid4())
    verify_resp = {
        'latest_receipt': str(uuid4()),
        'latest_receipt_info': {},
        'original_transaction_id': appstore_otid,
        'pending_renewal_info': {},
    }
    with pytest.raises(AppStoreException) as err:
        with patch.object(appstore_manager.appstore_client, 'verify_receipt', return_value=verify_resp):
            appstore_manager.update_subscription(our_otid, 'receipt-data')
    assert err


def test_update_subscription_basic_success(appstore_manager, user):
    otid = str(uuid4())
    org_item = appstore_manager.sub_dynamo.add(
        otid, user.id, AppStoreSubscriptionStatus.ACTIVE, '-', '-', '-', '-', pendulum.now('utc')
    )
    verify_resp = {
        'latest_receipt': str(uuid4()),
        'latest_receipt_info': {'foo': 'bar'},
        'original_transaction_id': otid,
        'pending_renewal_info': {'bar': 'foo'},
    }
    with patch.object(appstore_manager.appstore_client, 'verify_receipt', return_value=verify_resp):
        appstore_manager.update_subscription(otid, 'receipt-data')
    new_item = appstore_manager.sub_dynamo.get(otid)
    assert new_item['lastVerificationAt'] > org_item['lastVerificationAt']
    assert new_item['gsiK1SortKey'] > org_item['gsiK1SortKey']
    assert new_item == {
        **org_item,
        'latestReceipt': verify_resp['latest_receipt'],
        'latestReceiptInfo': verify_resp['latest_receipt_info'],
        'pendingRenewalInfo': verify_resp['pending_renewal_info'],
        'lastVerificationAt': new_item['lastVerificationAt'],
        'gsiK1SortKey': new_item['gsiK1SortKey'],
    }


def test_update_subscription_change_status(appstore_manager, user):
    otid = str(uuid4())
    now = pendulum.now('utc')
    org_item = appstore_manager.sub_dynamo.add(
        otid, user.id, AppStoreSubscriptionStatus.ACTIVE, '-', '-', '-', '-', now
    )
    verify_resp = {
        'latest_receipt': str(uuid4()),
        'latest_receipt_info': {'expires_date_ms': str(int(now.timestamp() * 1000))},
        'original_transaction_id': otid,
        'pending_renewal_info': {'bar': 'foo'},
    }
    with patch.object(appstore_manager.appstore_client, 'verify_receipt', return_value=verify_resp):
        appstore_manager.update_subscription(otid, 'receipt-data')
    new_item = appstore_manager.sub_dynamo.get(otid)
    assert new_item['lastVerificationAt'] > org_item['lastVerificationAt']
    assert new_item['gsiK1SortKey'] > org_item['gsiK1SortKey']
    assert new_item == {
        **org_item,
        'status': AppStoreSubscriptionStatus.EXPIRED,
        'latestReceipt': verify_resp['latest_receipt'],
        'latestReceiptInfo': verify_resp['latest_receipt_info'],
        'pendingRenewalInfo': verify_resp['pending_renewal_info'],
        'lastVerificationAt': new_item['lastVerificationAt'],
        'gsiK1SortKey': new_item['gsiK1SortKey'],
    }
