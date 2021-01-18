from uuid import uuid4

import pytest
from mock import patch

from app.mixins.view.enums import ViewedStatus, ViewType
from app.models.post.enums import PostType


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user
user3 = user


@pytest.fixture
def post(post_manager, user):
    yield post_manager.add_post(user, str(uuid4()), PostType.TEXT_ONLY, text='t')


@pytest.fixture
def chat(chat_manager, user, user2):
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        yield chat_manager.add_direct_chat(str(uuid4()), user.id, user2.id)


@pytest.fixture
def screen(screen_manager):
    yield screen_manager.init_screen(f'screen-name-{uuid4()}')


@pytest.mark.parametrize('model', pytest.lazy_fixture(['post', 'chat']))  # screens have no owner
def test_owner_cant_record_views_has_always_alread_viewed(model, user2):
    # check owner has always viewed it
    assert model.get_viewed_status(model.user_id) == ViewedStatus.VIEWED
    model.record_view_count(model.user_id, 5)
    assert model.get_viewed_status(model.user_id) == ViewedStatus.VIEWED


@pytest.mark.parametrize('model', pytest.lazy_fixture(['post', 'chat', 'screen']))
def test_record_and_get_views(model, user2, user3):
    # check users have not viewed it
    assert model.get_viewed_status(user2.id) == ViewedStatus.NOT_VIEWED
    assert model.get_viewed_status(user3.id) == ViewedStatus.NOT_VIEWED

    # record some views by the rando, check recorded to dynamo
    model.record_view_count(user2.id, 5)
    assert model.get_viewed_status(user2.id) == ViewedStatus.VIEWED
    view_item = model.view_dynamo.get_view(model.id, user2.id)
    assert view_item['viewCount'] == 5
    assert user2.id in view_item['sortKey']
    assert view_item['firstViewedAt']
    assert view_item['firstViewedAt'] == view_item['lastViewedAt']
    first_viewed_at = view_item['firstViewedAt']

    # record some more views by the rando, check recorded to dynamo
    model.record_view_count(user2.id, 3)
    assert model.get_viewed_status(user2.id) == ViewedStatus.VIEWED
    view_item = model.view_dynamo.get_view(model.id, user2.id)
    assert view_item['viewCount'] == 8
    assert view_item['firstViewedAt'] == first_viewed_at
    assert view_item['lastViewedAt'] > first_viewed_at

    # record views by the other user too, check their viewed status also changed
    model.record_view_count(user3.id, 3)
    assert model.get_viewed_status(user3.id) == ViewedStatus.VIEWED
    assert model.view_dynamo.get_view(model.id, user3.id)


def test_record_view_count_with_view_type(post, user2, user3):
    # check starting state
    assert post.get_viewed_status(user2.id) == ViewedStatus.NOT_VIEWED
    assert post.get_viewed_status(user3.id) == ViewedStatus.NOT_VIEWED
    assert post.view_dynamo.get_view(post.id, user2.id) is None
    assert post.view_dynamo.get_view(post.id, user3.id) is None

    # record views with focus view type, verify
    post.record_view_count(user3.id, 3, None, ViewType.FOCUS)
    assert post.get_viewed_status(user3.id) == ViewedStatus.VIEWED
    view_item = post.view_dynamo.get_view(post.id, user3.id)
    assert view_item['viewCount'] == 3
    assert view_item['focusViewCount'] == 3
    assert view_item.get('thumbnailViewCount', 0) == 0

    # record views with thumbnail view type, verify
    post.record_view_count(user2.id, 3, None, ViewType.THUMBNAIL)
    assert post.get_viewed_status(user2.id) == ViewedStatus.VIEWED
    view_item = post.view_dynamo.get_view(post.id, user2.id)
    assert view_item['viewCount'] == 3
    assert view_item['thumbnailViewCount'] == 3
    assert view_item.get('focusViewCount', 0) == 0
