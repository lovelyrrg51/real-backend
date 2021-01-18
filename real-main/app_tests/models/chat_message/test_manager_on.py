import logging
from uuid import uuid4

import pytest
from mock import patch


@pytest.fixture
def user1(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user1


@pytest.fixture
def chat(chat_manager, user1, user2):
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        yield chat_manager.add_direct_chat('cid', user1.id, user2.id)


@pytest.fixture
def message(chat_message_manager, chat, user1):
    yield chat_message_manager.add_chat_message(str(uuid4()), 'lore ipsum', chat.id, user1.id)


def test_on_flag_add(chat_message_manager, message, user2):
    # check & configure starting state
    assert message.refresh_item().item.get('flagCount', 0) == 0
    transact = message.chat.dynamo.transact_increment_user_count(message.chat_id)
    for _ in range(8):
        message.dynamo.client.transact_write_items([transact])
    assert message.chat.refresh_item().item['userCount'] == 10  # just above cutoff for one flag

    # messageprocess, verify flagCount is incremented & not force achived
    chat_message_manager.on_flag_add(message.id, new_item={})
    assert message.refresh_item().item.get('flagCount', 0) == 1


def test_on_flag_add_force_delete_by_crowdsourced_criteria(chat_message_manager, message, user2, caplog):
    # configure and check starting state
    assert message.refresh_item().item.get('flagCount', 0) == 0
    transact = message.chat.dynamo.transact_increment_user_count(message.chat_id)
    for _ in range(7):
        message.dynamo.client.transact_write_items([transact])
    assert message.chat.refresh_item().item['userCount'] == 9  # just below 10% cutoff for one flag

    # postprocess, verify flagCount is incremented and force archived
    with caplog.at_level(logging.WARNING):
        chat_message_manager.on_flag_add(message.id, new_item={})
    assert len(caplog.records) == 1
    assert 'Force deleting chat message' in caplog.records[0].msg
    assert message.refresh_item().item is None


def test_on_chat_delete_delete_messages(chat_message_manager, user1, chat):
    # add two messsages
    message_id_1, message_id_2 = str(uuid4()), str(uuid4())
    chat_message_manager.add_chat_message(message_id_1, 'lore', chat.id, user1.id)
    chat_message_manager.add_chat_message(message_id_2, 'ipsum', chat.id, user1.id)
    assert chat_message_manager.get_chat_message(message_id_1)
    assert chat_message_manager.get_chat_message(message_id_2)

    # trigger, verify messages are gone
    chat_message_manager.on_chat_delete_delete_messages(chat.id, old_item=chat.item)
    assert chat_message_manager.get_chat_message(message_id_1) is None
    assert chat_message_manager.get_chat_message(message_id_2) is None
