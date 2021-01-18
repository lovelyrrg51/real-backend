from unittest.mock import patch
from uuid import uuid4

import pytest

from app.mixins.flag.exceptions import FlagException
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
def comment(comment_manager, post, user):
    yield comment_manager.add_comment(str(uuid4()), post.id, user.id, 'lore ipsum')


@pytest.fixture
def chat(chat_manager, user, user2):
    group_chat = chat_manager.add_group_chat(str(uuid4()), user)
    with patch.object(chat_manager, 'validate_dating_match_chat', return_value=True):
        group_chat.add(user, [user2.id])
    yield group_chat


@pytest.fixture
def message(chat_message_manager, chat, user):
    yield chat_message_manager.add_chat_message(str(uuid4()), 'lore ipsum', chat.id, user.id)


user2 = user


@pytest.mark.parametrize('model', pytest.lazy_fixture(['post', 'comment', 'message']))
def test_flag_success(model, user2):
    # check starting state
    assert model.item.get('flagCount', 0) == 0
    assert len(list(model.flag_dynamo.generate_keys_by_item(model.id))) == 0

    # flag it, verify count incremented in memory but not yet in DB
    model.flag(user2)
    assert model.item.get('flagCount', 0) == 1
    assert model.refresh_item().item.get('flagCount', 0) == 0
    assert len(list(model.flag_dynamo.generate_keys_by_item(model.id))) == 1

    # verify we can't flag the post second time
    with pytest.raises(FlagException, match='already been flagged'):
        model.flag(user2)
    assert model.item.get('flagCount', 0) == 0
    assert model.refresh_item().item.get('flagCount', 0) == 0


@pytest.mark.parametrize('model', pytest.lazy_fixture(['post', 'comment', 'message']))
def test_cant_flag_our_own_model(model, user):
    with pytest.raises(FlagException, match='flag their own'):
        model.flag(user)
    assert model.item.get('flagCount', 0) == 0
    assert model.refresh_item().item.get('flagCount', 0) == 0
    assert list(model.flag_dynamo.generate_keys_by_item(model.id)) == []


@pytest.mark.parametrize('model', pytest.lazy_fixture(['post', 'comment', 'message']))
def test_cant_flag_model_of_user_thats_blocking_us(model, user, user2, block_manager):
    block_manager.block(user, user2)
    with pytest.raises(FlagException, match='has been blocked by owner'):
        model.flag(user2)
    assert model.item.get('flagCount', 0) == 0
    assert model.refresh_item().item.get('flagCount', 0) == 0
    assert list(model.flag_dynamo.generate_keys_by_item(model.id)) == []


@pytest.mark.parametrize('model', pytest.lazy_fixture(['post', 'comment', 'message']))
def test_cant_flag_model_of_user_we_are_blocking(model, user, user2, block_manager):
    block_manager.block(user2, user)
    with pytest.raises(FlagException, match='has blocked owner'):
        model.flag(user2)
    assert model.item.get('flagCount', 0) == 0
    assert model.refresh_item().item.get('flagCount', 0) == 0
    assert list(model.flag_dynamo.generate_keys_by_item(model.id)) == []


@pytest.mark.parametrize('model', pytest.lazy_fixture(['post', 'comment', 'message']))
def test_unflag(model, user2):
    # flag the model and do the post-processing counter increment
    model.flag(user2)
    model.dynamo.increment_flag_count(model.id)
    assert model.item.get('flagCount', 0) == 1
    assert model.refresh_item().item.get('flagCount', 0) == 1
    assert len(list(model.flag_dynamo.generate_keys_by_item(model.id))) == 1

    # unflag, verify counter decremented in mem but not yet in dynamo
    model.unflag(user2.id)
    assert model.item.get('flagCount', 0) == 0
    assert model.refresh_item().item.get('flagCount', 0) == 1
    assert len(list(model.flag_dynamo.generate_keys_by_item(model.id))) == 0

    # verify can't unflag if we haven't flagged
    with pytest.raises(FlagException, match='not been flagged'):
        model.unflag(user2.id)


@pytest.mark.parametrize('model', pytest.lazy_fixture(['post', 'comment']))
def test_is_crowdsourced_forced_removal_criteria_met_post(model, user2):
    # should archive if over 5 users have viewed the model and more than 10% have flagged it
    # one flag, verify shouldn't force-archive
    model.dynamo.increment_flag_count(model.id)
    model.refresh_item()
    assert model.is_crowdsourced_forced_removal_criteria_met() is False

    # with 5 views, verify still shouldn't force-archive
    with patch.object(model.__class__, 'viewed_by_count', 5):
        assert model.is_crowdsourced_forced_removal_criteria_met() is False

    # with 6 views, verify should force-archive now
    with patch.object(model.__class__, 'viewed_by_count', 6):
        assert model.is_crowdsourced_forced_removal_criteria_met() is True
