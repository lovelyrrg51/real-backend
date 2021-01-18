import logging
from unittest.mock import patch
from uuid import uuid4

import pytest

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
    yield post_manager.add_post(user, str(uuid4()), PostType.TEXT_ONLY, text='go go')


@pytest.fixture
def comment(comment_manager, post, user2):
    yield comment_manager.add_comment(str(uuid4()), post.id, user2.id, 'lore ipsum')


@pytest.fixture
def flag_item(comment, user3):
    comment.flag(user3)
    yield comment.flag_dynamo.get(comment.id, user3.id)


@pytest.fixture
def post_owner_flag_item(comment, user):
    comment.flag(user)
    yield comment.flag_dynamo.get(comment.id, user.id)


def test_on_flag_add(comment_manager, comment, caplog, flag_item):
    # check starting state
    assert comment.refresh_item().item.get('flagCount', 0) == 0

    # commentprocess, verify flagCount is incremented & not force deleted
    with caplog.at_level(logging.WARNING):
        comment_manager.on_flag_add(comment.id, new_item=flag_item)
    assert len(caplog.records) == 0
    assert comment.refresh_item().item.get('flagCount', 0) == 1


def test_on_flag_add_force_delete_by_post_owner(comment_manager, comment, user, caplog, post_owner_flag_item):
    # configure and check starting state
    assert comment.refresh_item().item.get('flagCount', 0) == 0

    # commentprocess, verify comment is force-deleted
    with caplog.at_level(logging.WARNING):
        comment_manager.on_flag_add(comment.id, new_item=post_owner_flag_item)
    assert len(caplog.records) == 1
    assert 'Force deleting comment' in caplog.records[0].msg
    assert comment.refresh_item().item is None


def test_on_flag_add_force_delete_by_admin(comment_manager, comment, user3, caplog, flag_item):
    # configure and check starting state
    assert comment.refresh_item().item.get('flagCount', 0) == 0

    # commentprocess, verify comment is force-deleted
    with patch.object(comment_manager, 'flag_admin_usernames', (user3.username,)):
        with caplog.at_level(logging.WARNING):
            comment_manager.on_flag_add(comment.id, new_item=flag_item)
    assert len(caplog.records) == 1
    assert 'Force deleting comment' in caplog.records[0].msg
    assert comment.refresh_item().item is None


def test_on_flag_add_force_delete_by_crowdsourced_criteria(comment_manager, comment, caplog, flag_item):
    # configure and check starting state
    assert comment.refresh_item().item.get('flagCount', 0) == 0
    for _ in range(6):
        comment.post.dynamo.increment_viewed_by_count(comment.post.id)

    # commentprocess, verify flagCount is incremented and force archived
    with caplog.at_level(logging.WARNING):
        comment_manager.on_flag_add(comment.id, new_item=flag_item)
    assert len(caplog.records) == 1
    assert 'Force deleting comment' in caplog.records[0].msg
    assert comment.refresh_item().item is None


def test_on_user_delete_delete_all_by_user(comment_manager, user, post, user2, user3):
    # add a comment by an unrelated user for distraction
    comment_other = comment_manager.add_comment('coid', post.id, user2.id, 'lore')

    # add two comments by our target user
    comment_1 = comment_manager.add_comment('cid1', post.id, user3.id, 'lore')
    comment_2 = comment_manager.add_comment('cid2', post.id, user3.id, 'lore')

    # delete all the comments by the user, verify it worked
    comment_manager.on_user_delete_delete_all_by_user(user3.id, old_item=user3.item)
    assert comment_manager.get_comment(comment_1.id) is None
    assert comment_manager.get_comment(comment_2.id) is None

    # verify the unrelated comment was untouched
    assert comment_manager.get_comment(comment_other.id)
