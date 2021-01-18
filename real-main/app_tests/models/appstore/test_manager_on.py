from unittest.mock import patch
from uuid import uuid4

import pytest


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user1 = user
user2 = user


def test_on_user_delete_delete_all_by_user(appstore_manager, user1, user2):
    verify_1 = {
        'latest_receipt': str(uuid4()),
        'latest_receipt_info': {},
        'original_transaction_id': str(uuid4()),
        'pending_renewal_info': {},
    }
    verify_2 = {
        'latest_receipt': str(uuid4()),
        'latest_receipt_info': {},
        'original_transaction_id': str(uuid4()),
        'pending_renewal_info': {},
    }

    # add one receipt by each user, verify exist
    with patch.object(appstore_manager.appstore_client, 'verify_receipt', return_value=verify_1):
        appstore_manager.add_receipt(str(uuid4()), user1.id)
    with patch.object(appstore_manager.appstore_client, 'verify_receipt', return_value=verify_2):
        appstore_manager.add_receipt(str(uuid4()), user2.id)
    assert len(list(appstore_manager.sub_dynamo.generate_keys_by_user(user1.id))) == 1
    assert len(list(appstore_manager.sub_dynamo.generate_keys_by_user(user2.id))) == 1

    # trigger for one user, verify
    appstore_manager.on_user_delete_delete_all_by_user(user2.id, old_item=user2.item)
    assert len(list(appstore_manager.sub_dynamo.generate_keys_by_user(user1.id))) == 1
    assert len(list(appstore_manager.sub_dynamo.generate_keys_by_user(user2.id))) == 0

    # trigger for the other user, verify
    appstore_manager.on_user_delete_delete_all_by_user(user1.id, old_item=user1.item)
    assert len(list(appstore_manager.sub_dynamo.generate_keys_by_user(user1.id))) == 0
    assert len(list(appstore_manager.sub_dynamo.generate_keys_by_user(user2.id))) == 0
