import logging
import uuid

import pytest

from app.models.post.enums import PostType


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user
user3 = user


@pytest.fixture
def post(post_manager, user):
    yield post_manager.add_post(user, str(uuid.uuid4()), PostType.TEXT_ONLY, text='t')


post2 = post


def test_record_view_count_logs_warning_for_non_completed_posts(post, user2, caplog):
    # verify no warning for a completed post
    with caplog.at_level(logging.WARNING):
        post.record_view_count(user2.id, 2)
    assert len(caplog.records) == 0

    # verify warning for a non-completed post
    post.archive()
    with caplog.at_level(logging.WARNING):
        post.record_view_count(user2.id, 2)
    assert len(caplog.records) == 1
    assert user2.id in caplog.records[0].msg
    assert post.id in caplog.records[0].msg


def test_record_view_count_records_to_original_post_as_well(post, post2, user2):
    # verify a rando's view is recorded locally and goes up to the orginal
    assert post.view_dynamo.get_view(post.id, user2.id) is None
    assert post2.view_dynamo.get_view(post2.id, user2.id) is None
    post.item['originalPostId'] = post2.id
    post.record_view_count(user2.id, 1)
    assert post.view_dynamo.get_view(post.id, user2.id)
    assert post2.view_dynamo.get_view(post2.id, user2.id)
