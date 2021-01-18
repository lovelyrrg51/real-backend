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
def chat(chat_manager, user, user2):
    group_chat = chat_manager.add_group_chat(str(uuid4()), user)
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        group_chat.add(user, [user2.id])
    yield group_chat


@pytest.fixture
def screen(screen_manager):
    yield screen_manager.init_screen(f'screen-name-{uuid4()}')


user2 = user
post2 = post
chat2 = chat
screen2 = screen


@pytest.mark.parametrize('manager', pytest.lazy_fixture(['post_manager', 'chat_manager', 'screen_manager']))
def test_record_views_implemented(manager):
    # should not error out
    manager.record_views(['iid1', 'iid2'], 'uid')


@pytest.mark.parametrize(
    'manager, model1, model2',
    [
        pytest.lazy_fixture(['post_manager', 'post', 'post2']),
        pytest.lazy_fixture(['chat_manager', 'chat', 'chat2']),
        # doesn't apply to screen views as there is no associated item
    ],
)
def test_on_item_delete_delete_views(manager, model1, model2, user, user2):
    # configure starting state, verify
    model1.record_view_count(user.id, 3)
    model2.record_view_count(user.id, 3)
    model1.record_view_count(user2.id, 3)
    assert manager.view_dynamo.get_view(model1.id, user.id)
    assert manager.view_dynamo.get_view(model2.id, user.id)
    assert manager.view_dynamo.get_view(model1.id, user2.id)
    assert manager.view_dynamo.get_view(model2.id, user2.id) is None

    # delete views for one model, verify
    manager.on_item_delete_delete_views(model1.id, old_item=model1.item)
    assert manager.view_dynamo.get_view(model1.id, user.id) is None
    assert manager.view_dynamo.get_view(model2.id, user.id)
    assert manager.view_dynamo.get_view(model1.id, user2.id) is None
    assert manager.view_dynamo.get_view(model2.id, user2.id) is None

    # delete views for the other model, verify
    manager.on_item_delete_delete_views(model2.id, old_item=model2.item)
    assert manager.view_dynamo.get_view(model1.id, user.id) is None
    assert manager.view_dynamo.get_view(model2.id, user.id) is None
    assert manager.view_dynamo.get_view(model1.id, user2.id) is None
    assert manager.view_dynamo.get_view(model2.id, user2.id) is None


@pytest.mark.parametrize(
    'manager, model1, model2',
    [
        pytest.lazy_fixture(['post_manager', 'post', 'post2']),
        pytest.lazy_fixture(['chat_manager', 'chat', 'chat2']),
        pytest.lazy_fixture(['screen_manager', 'screen', 'screen2']),
    ],
)
def test_on_user_delete_delete_views(manager, model1, model2, user, user2):
    # configure starting state, verify
    model1.record_view_count(user.id, 3)
    model2.record_view_count(user.id, 3)
    model1.record_view_count(user2.id, 3)
    assert manager.view_dynamo.get_view(model1.id, user.id)
    assert manager.view_dynamo.get_view(model2.id, user.id)
    assert manager.view_dynamo.get_view(model1.id, user2.id)
    assert manager.view_dynamo.get_view(model2.id, user2.id) is None

    # delete views for one user, verify
    manager.on_user_delete_delete_views(user.id, old_item=user.item)
    assert manager.view_dynamo.get_view(model1.id, user.id) is None
    assert manager.view_dynamo.get_view(model2.id, user.id) is None
    assert manager.view_dynamo.get_view(model1.id, user2.id)
    assert manager.view_dynamo.get_view(model2.id, user2.id) is None

    # delete views for the other user, verify
    manager.on_user_delete_delete_views(user2.id, old_item=user2.item)
    assert manager.view_dynamo.get_view(model1.id, user.id) is None
    assert manager.view_dynamo.get_view(model2.id, user.id) is None
    assert manager.view_dynamo.get_view(model1.id, user2.id) is None
    assert manager.view_dynamo.get_view(model2.id, user2.id) is None
