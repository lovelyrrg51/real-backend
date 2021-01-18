from unittest.mock import patch
from uuid import uuid4

import pendulum
import pytest

from app.mixins.view.enums import ViewType
from app.models.post.enums import PostStatus, PostType


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def post(post_manager, user):
    now = pendulum.parse('2020-06-09T00:00:00Z')  # exact begining of day for easy trending point math
    yield post_manager.add_post(user, str(uuid4()), PostType.TEXT_ONLY, text='go go', now=now)


user2 = user


def test_on_post_view_change_update_trending_post_dne(post_manager, post, user2):
    # sneak behind the post and remove it directly from the DB
    post.dynamo.delete_post(post.id)
    assert post_manager.get_post(post.id) is None
    org_trending_score = post.refresh_trending_item().trending_score

    # simulate calling for an add, verify no change to trending
    post.record_view_count(user2.id, 3)
    org_view_item = post.view_dynamo.get_view(post.id, user2.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=org_view_item)
    assert post.refresh_trending_item().trending_score == org_trending_score

    # simulate calling for an edit, verify no change to trending
    post.record_view_count(user2.id, 2)
    new_view_item = post.view_dynamo.get_view(post.id, user2.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item, old_item=org_view_item)
    assert post.refresh_trending_item().trending_score == org_trending_score


def test_on_post_view_change_update_trending_post_not_completed(post_manager, post, user2):
    # sneak behind the post and change its status directly in the DB
    post.dynamo.set_post_status(post.item, PostStatus.ARCHIVED)
    assert post.refresh_item().status == PostStatus.ARCHIVED
    assert post.refresh_trending_item().trending_score == 1

    # simulate calling for an add, verify no change to trending
    post.record_view_count(user2.id, 3)
    org_view_item = post.view_dynamo.get_view(post.id, user2.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=org_view_item)
    assert post.refresh_trending_item().trending_score == 1

    # simulate calling for an edit, verify no change to trending
    post.record_view_count(user2.id, 2)
    new_view_item = post.view_dynamo.get_view(post.id, user2.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item, old_item=org_view_item)
    assert post.refresh_trending_item().trending_score == 1


def test_on_post_view_change_update_trending_viewed_by_post_owner(post_manager, post, user):
    # check we have just the free trending points given to all new posts
    assert post.refresh_trending_item().trending_score == 1

    # simulate calling for an add, verify no change to trending
    post.record_view_count(user.id, 3)
    org_view_item = post.view_dynamo.get_view(post.id, user.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=org_view_item)
    assert post.refresh_trending_item().trending_score == 1

    # simulate calling for an edit, verify no change to trending
    post.record_view_count(user.id, 2)
    new_view_item = post.view_dynamo.get_view(post.id, user.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item, old_item=org_view_item)
    assert post.refresh_trending_item().trending_score == 1


@pytest.mark.parametrize('view_type', [None, ViewType.THUMBNAIL, ViewType.FOCUS])
def test_on_post_view_change_update_trending_view_item_inserted_general_success_case(
    post_manager, post, user, user2, view_type
):
    viewed_at = pendulum.parse('2020-06-09T00:00:00Z')  # exact begining of day so trending posts haven't inflated
    assert post.refresh_trending_item().trending_score == 1

    # simulate calling for an add, verify post gets some trending
    post.record_view_count(user2.id, 3, view_type=view_type, viewed_at=viewed_at)
    org_view_item = post.view_dynamo.get_view(post.id, user2.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=org_view_item)
    assert post.refresh_trending_item().trending_score == 1 + post.get_trending_multiplier(view_type=view_type)

    # simulate calling a second time, verify doesn't cause any additional trending
    post.record_view_count(user2.id, 2, view_type=view_type, viewed_at=viewed_at)
    new_view_item = post.view_dynamo.get_view(post.id, user2.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item, old_item=org_view_item)
    assert post.refresh_trending_item().trending_score == 1 + post.get_trending_multiplier(view_type=view_type)


@pytest.mark.parametrize('org_view_type', [None, ViewType.THUMBNAIL])
def test_on_post_view_change_update_trending_view_item_modified_by_first_focus_view(
    post_manager, post, user, user2, org_view_type
):
    viewed_at = pendulum.parse('2020-06-09T00:00:00Z')  # exact begining of day so trending posts haven't inflated
    assert post.refresh_trending_item().trending_score == 1

    # first trigger for adding the view record in the first place, verify adds some trending
    post.record_view_count(user2.id, 3, view_type=org_view_type, viewed_at=viewed_at)
    new_view_item = post.view_dynamo.get_view(post.id, user2.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item)
    assert post.refresh_trending_item().trending_score == 1 + 1

    # then trigger for editing the view record with a FOCUS view, verify adds some trending
    post.record_view_count(user2.id, 2, view_type=ViewType.FOCUS, viewed_at=viewed_at)
    old_view_item = new_view_item
    new_view_item = post.view_dynamo.get_view(post.id, user2.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item, old_item=old_view_item)
    assert post.refresh_trending_item().trending_score == 1 + 1 + 2

    # trigger for editing with FOCUS view again, verify no new trending this time
    post.record_view_count(user2.id, 2, view_type=ViewType.FOCUS, viewed_at=viewed_at)
    old_view_item = new_view_item
    new_view_item = post.view_dynamo.get_view(post.id, user2.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item, old_item=old_view_item)
    assert post.refresh_trending_item().trending_score == 1 + 1 + 2


@pytest.mark.parametrize('second_view_type', [None, ViewType.THUMBNAIL])
def test_on_post_view_change_update_trending_view_item_modified_by_first_non_focus_view(
    post_manager, post, user, user2, second_view_type
):
    viewed_at = pendulum.parse('2020-06-09T00:00:00Z')  # exact begining of day so trending posts haven't inflated
    assert post.refresh_trending_item().trending_score == 1

    # first trigger for adding the view record in the first place with a FOCUS view, verify adds some trending
    post.record_view_count(user2.id, 3, view_type=ViewType.FOCUS, viewed_at=viewed_at)
    new_view_item = post.view_dynamo.get_view(post.id, user2.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item)
    assert post.refresh_trending_item().trending_item
    assert post.trending_score == 1 + 2

    # then trigger for editing the view record with the non-focus view type, verify adds some trending
    post.record_view_count(user2.id, 2, view_type=second_view_type, viewed_at=viewed_at)
    old_view_item = new_view_item
    new_view_item = post.view_dynamo.get_view(post.id, user2.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item, old_item=old_view_item)
    assert post.refresh_trending_item().trending_score == 1 + 2 + 1

    # then trigger for editing the view record with the non-focus view type, verify no new trending this time
    post.record_view_count(user2.id, 2, view_type=second_view_type, viewed_at=viewed_at)
    old_view_item = new_view_item
    new_view_item = post.view_dynamo.get_view(post.id, user2.id)
    post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item, old_item=old_view_item)
    assert post.refresh_trending_item().trending_score == 1 + 2 + 1


def test_on_post_view_change_update_trending_user_updated_only_if_post_updated(post_manager, post, user, user2):
    viewed_at = pendulum.parse('2020-06-09T00:00:00Z')  # exact begining of day so trending posts haven't inflated
    assert post.refresh_trending_item().trending_score == 1
    assert user.refresh_trending_item().trending_score is None

    # set up parameters for a non-owner first view
    post.record_view_count(user2.id, 3, viewed_at=viewed_at)
    new_view_item = post.view_dynamo.get_view(post.id, user2.id)

    # trigger with post.trending_increment_score succeeding, verify user trending also updated
    with patch.object(post, 'trending_increment_score', return_value=True):
        with patch.object(post_manager, 'get_post', return_value=post):
            post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item)
    assert user.refresh_trending_item().trending_score == 1

    # trigger with post.trending_increment_score failing, verify user trending doesn't change
    with patch.object(post, 'trending_increment_score', return_value=False):
        with patch.object(post_manager, 'get_post', return_value=post):
            post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item)
    assert user.refresh_trending_item().trending_score == 1

    # trigger with post.trending_increment_score succeeding, verify user trending updated
    with patch.object(post, 'trending_increment_score', return_value=True):
        with patch.object(post_manager, 'get_post', return_value=post):
            post_manager.on_post_view_change_update_trending(post.id, new_item=new_view_item)
    assert user.refresh_trending_item().trending_score == 1 + 1
