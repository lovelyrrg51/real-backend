import logging
from uuid import uuid4

import pytest
from mock import patch

from app.models.post.enums import PostType


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def post(post_manager, user):
    yield post_manager.add_post(user, str(uuid4()), PostType.TEXT_ONLY, text='t')


@pytest.fixture
def comment(comment_manager, user, post):
    yield comment_manager.add_comment(str(uuid4()), post.id, user.id, text='whit or lack thereof')


@pytest.fixture
def chat(chat_manager, user, user2, user3):
    group_chat = chat_manager.add_group_chat(str(uuid4()), user)
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        group_chat.add(user, [user2.id, user3.id])
    yield group_chat


@pytest.fixture
def message(chat_message_manager, chat, user):
    yield chat_message_manager.add_chat_message(str(uuid4()), 'lore ipsum', chat.id, user.id)


user2 = user
user3 = user
post2 = post
comment2 = comment
message2 = message
chat2 = chat


@pytest.mark.parametrize(
    'manager, model',
    [
        pytest.lazy_fixture(['post_manager', 'post']),
        pytest.lazy_fixture(['comment_manager', 'comment']),
        pytest.lazy_fixture(['chat_message_manager', 'message']),
        pytest.lazy_fixture(['chat_manager', 'chat']),
    ],
)
def test_on_flag_delete(manager, model, caplog):
    # configure and check starting state
    manager.dynamo.increment_flag_count(model.id)
    assert model.refresh_item().item.get('flagCount', 0) == 1

    # postprocess, verify flagCount is decremented
    manager.on_flag_delete(model.id, model.item)
    assert model.refresh_item().item.get('flagCount', 0) == 0

    # postprocess again, verify fails softly
    with caplog.at_level(logging.WARNING):
        manager.on_flag_delete(model.id, model.item)
    assert len(caplog.records) == 1
    assert 'Failed to decrement flagCount' in caplog.records[0].msg
    assert model.refresh_item().item.get('flagCount', 0) == 0


@pytest.mark.parametrize(
    'manager, model1, model2',
    [
        pytest.lazy_fixture(['post_manager', 'post', 'post2']),
        pytest.lazy_fixture(['comment_manager', 'comment', 'comment2']),
        pytest.lazy_fixture(['chat_message_manager', 'message', 'message2']),
        pytest.lazy_fixture(['chat_manager', 'chat', 'chat2']),
    ],
)
def test_on_item_delete_delete_flags(manager, model1, model2, user2, user3):
    # user2 flags both those models, user3 flags one model
    model1.flag(user2)
    model2.flag(user2)
    model1.flag(user3)
    assert len(list(manager.flag_dynamo.generate_keys_by_user(user2.id))) == 2
    assert len(list(manager.flag_dynamo.generate_keys_by_user(user3.id))) == 1

    # react to a delete of the first model, verify
    manager.on_item_delete_delete_flags(model1.id, old_item=model1.item)
    assert len(list(manager.flag_dynamo.generate_keys_by_user(user2.id))) == 1
    assert len(list(manager.flag_dynamo.generate_keys_by_user(user3.id))) == 0

    # react to a delete of the second model, verify
    manager.on_item_delete_delete_flags(model2.id, old_item=model2.item)
    assert len(list(manager.flag_dynamo.generate_keys_by_user(user2.id))) == 0
    assert len(list(manager.flag_dynamo.generate_keys_by_user(user3.id))) == 0


@pytest.mark.parametrize(
    'manager, model1, model2',
    [
        pytest.lazy_fixture(['post_manager', 'post', 'post2']),
        pytest.lazy_fixture(['comment_manager', 'comment', 'comment2']),
        pytest.lazy_fixture(['chat_message_manager', 'message', 'message2']),
        pytest.lazy_fixture(['chat_manager', 'chat', 'chat2']),
    ],
)
def test_on_user_delete_delete_flags(manager, model1, model2, user2, user3):
    # user2 flags both those models, user3 flags one model
    model1.flag(user2)
    model2.flag(user2)
    model1.flag(user3)
    assert len(list(manager.flag_dynamo.generate_keys_by_user(user2.id))) == 2
    assert len(list(manager.flag_dynamo.generate_keys_by_user(user3.id))) == 1

    # react to a delete of the user2, verify
    manager.on_user_delete_delete_flags(user2.id, old_item=user2.item)
    assert len(list(manager.flag_dynamo.generate_keys_by_user(user2.id))) == 0
    assert len(list(manager.flag_dynamo.generate_keys_by_user(user3.id))) == 1

    # react to a delete of the second model, verify
    manager.on_item_delete_delete_flags(model2.id, old_item=model2.item)
    assert len(list(manager.flag_dynamo.generate_keys_by_user(user2.id))) == 0
    assert len(list(manager.flag_dynamo.generate_keys_by_user(user3.id))) == 1
