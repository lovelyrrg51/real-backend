import uuid

import pytest

from app.models.like.enums import LikeStatus
from app.models.like.exceptions import NotLikedWithStatus
from app.models.post.enums import PostType


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


other_user = user


@pytest.fixture
def post(dynamo_client, like_manager, user, post_manager):
    yield post_manager.add_post(user, 'pid', PostType.TEXT_ONLY, text='lore ipsum')


@pytest.fixture
def like(like_manager, post, user):
    like_manager.like_post(user, post, LikeStatus.ANONYMOUSLY_LIKED)
    yield like_manager.get_like(user.id, post.id)


def test_dislike(like_manager, like):
    # verify initial state
    liked_by_user_id = like.item['likedByUserId']
    post_id = like.item['postId']
    assert like.item['likeStatus'] == LikeStatus.ANONYMOUSLY_LIKED

    # dislike, verify
    like.dislike()
    assert like_manager.get_like(liked_by_user_id, post_id) is None


def test_dislike_fail_not_liked_with_status(like_manager, like, other_user, post):
    assert like.item['likeStatus'] == LikeStatus.ANONYMOUSLY_LIKED

    # add a like to the post of the other status so that we don't have a problem decrementing that counter
    like_manager.like_post(other_user, post, LikeStatus.ONYMOUSLY_LIKED)

    # change the in-memory status so its different than the db one
    like.item['likeStatus'] = LikeStatus.ONYMOUSLY_LIKED

    # verify fails because of the mismatch (doesnt know what counter to decrement)
    with pytest.raises(NotLikedWithStatus):
        like.dislike()
