import uuid

import pendulum
import pytest
from mock import patch

from app.models.comment.exceptions import CommentException
from app.models.post.enums import PostType
from app.models.user.enums import UserPrivacyStatus


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user
user3 = user


@pytest.fixture
def post(post_manager, user):
    yield post_manager.add_post(user, 'pid', PostType.TEXT_ONLY, text='go go')


@pytest.fixture
def post_image(post_manager, user):
    yield post_manager.add_post(user, 'pid_1', PostType.IMAGE)


def test_add_comment(comment_manager, user, post):
    comment_id = 'cid'

    # check our starting state
    assert comment_manager.get_comment(comment_id) is None

    # add the comment, verify
    username = user.item['username']
    text = f'hey @{username}'
    now = pendulum.now('utc')
    comment = comment_manager.add_comment(comment_id, post.id, user.id, text, now=now)
    assert comment.id == comment_id
    assert comment.item['postId'] == post.id
    assert comment.item['userId'] == user.id
    assert comment.item['text'] == text
    assert comment.item['textTags'] == [{'tag': f'@{username}', 'userId': user.id}]
    assert comment.item['commentedAt'] == now.to_iso8601_string()

    # check the post counter incremented, no new comment acitivy b/c the post owner commented
    post.refresh_item()
    assert post.item.get('hasNewCommentActivity', False) is False
    user.refresh_item()
    assert user.item.get('postHasNewCommentActivityCount', 0) == 0


def test_add_comment_cant_reuse_ids(comment_manager, user, post):
    comment_id = 'cid'
    text = 'lore'

    # add a comment, verify
    comment = comment_manager.add_comment(comment_id, post.id, user.id, text)
    assert comment.id == comment_id

    # verify we can't add another
    with pytest.raises(CommentException):
        comment_manager.add_comment(comment_id, post.id, user.id, text)


def test_cant_comment_to_post_that_doesnt_exist(comment_manager, user):
    # verify we can't add another
    with pytest.raises(CommentException):
        comment_manager.add_comment('cid', 'dne-pid', user.id, 't')


def test_cant_comment_on_post_with_comments_disabled(comment_manager, user, post):
    comment_id = 'cid'

    # disable comments on the post, verify we cannot add a comment
    post.set(comments_disabled=True)
    with pytest.raises(CommentException):
        comment_manager.add_comment(comment_id, post.id, user.id, 't')

    # enable comments on the post, verify we now can comment
    post.set(comments_disabled=False)
    comment = comment_manager.add_comment(comment_id, post.id, user.id, 't')
    assert comment.id == comment_id


def test_cant_comment_if_block_exists_with_post_owner(comment_manager, user, post, block_manager, user2):
    comment_id = 'cid'
    commenter = user2

    # owner blocks commenter, verify cannot comment
    block_manager.block(user, commenter)
    with pytest.raises(CommentException):
        comment_manager.add_comment(comment_id, post.id, commenter.id, 't')

    # owner unblocks commenter, commenter blocks owner, verify cannot comment
    block_manager.unblock(user, commenter)
    block_manager.block(commenter, user)
    with pytest.raises(CommentException):
        comment_manager.add_comment(comment_id, post.id, commenter.id, 't')

    # we commenter unblocks owner, verify now can comment
    block_manager.unblock(commenter, user)
    comment = comment_manager.add_comment(comment_id, post.id, commenter.id, 't')
    assert comment.id == comment_id


def test_can_comment_if_dating_is_matched(comment_manager, user, user2, post_image):
    comment_id = 'cid'

    with patch.object(comment_manager, 'validate_dating_match_comment', return_value=True):
        comment = comment_manager.add_comment(comment_id, post_image.id, user2.id, 't')
    assert comment.id == comment_id
    assert comment.item['postId'] == post_image.id
    assert comment.item['userId'] == user2.id
    assert comment.item['text'] == 't'


def test_cant_comment_if_dating_is_not_matched(comment_manager, user, user2, post_image):
    comment_id = 'cid'

    with patch.object(comment_manager, 'validate_dating_match_comment', return_value=False):
        with pytest.raises(CommentException):
            comment_manager.add_comment(comment_id, post_image.id, user2.id, 't')


def test_non_follower_cant_comment_if_private_post_owner(comment_manager, user, post, follower_manager, user2):
    comment_id = 'cid'
    commenter = user2

    # post owner goes private
    user.set_privacy_status(UserPrivacyStatus.PRIVATE)

    # verify we can't comment on their post
    with pytest.raises(CommentException):
        comment_manager.add_comment(comment_id, post.id, commenter.id, 't')

    # request to follow, verify can't comment
    follower_manager.request_to_follow(commenter, user)
    with pytest.raises(CommentException):
        comment_manager.add_comment(comment_id, post.id, commenter.id, 't')

    # deny the follow request, verify can't comment
    follower_manager.get_follow(commenter.id, user.id).deny()
    with pytest.raises(CommentException):
        comment_manager.add_comment(comment_id, post.id, commenter.id, 't')

    # accept the follow request, verify can comment
    follower_manager.get_follow(commenter.id, user.id).accept()
    comment = comment_manager.add_comment(comment_id, post.id, commenter.id, 't')
    assert comment.id == comment_id


def test_private_user_can_comment_on_own_post(comment_manager, user, post):
    comment_id = 'cid'

    # post owner goes private
    user.set_privacy_status(UserPrivacyStatus.PRIVATE)

    comment = comment_manager.add_comment(comment_id, post.id, user.id, 't')
    assert comment.id == comment_id


def test_delete_all_on_post(comment_manager, user, post, post_manager, user2, user3):
    # add another post, add a comment on it for distraction
    post_other = post_manager.add_post(user, 'pid-other', PostType.TEXT_ONLY, text='go go')
    comment_other = comment_manager.add_comment('coid', post_other.id, user.id, 'lore')

    # add two comments on the target post
    comment_1 = comment_manager.add_comment('cid1', post.id, user2.id, 'lore')
    comment_2 = comment_manager.add_comment('cid2', post.id, user3.id, 'lore')

    # delete all the comments on the post, verify it worked
    comment_manager.delete_all_on_post(post.id)
    assert comment_manager.get_comment(comment_1.id) is None
    assert comment_manager.get_comment(comment_2.id) is None

    # verify the unrelated comment was untouched
    assert comment_manager.get_comment(comment_other.id)
