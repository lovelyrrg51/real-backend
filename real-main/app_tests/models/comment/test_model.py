import uuid

import pytest

from app.models.comment.exceptions import CommentException
from app.models.post.enums import PostType


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user
user3 = user


@pytest.fixture
def comment(user, post_manager, comment_manager):
    post = post_manager.add_post(user, str(uuid.uuid4()), PostType.TEXT_ONLY, text='go go')
    yield comment_manager.add_comment(str(uuid.uuid4()), post.id, user.id, 'run far')


comment2 = comment


def test_serialize(comment_manager, comment, user):
    # serialize as the comment's author
    resp = comment.serialize(user.id)
    assert resp.pop('commentedBy')['userId'] == user.id
    assert resp == comment.item

    # serialize as another user
    other_user_id = 'ouid'
    resp = comment.serialize(other_user_id)
    assert resp.pop('commentedBy')['userId'] == user.id
    assert resp == comment.item


def test_delete(comment, post_manager, comment_manager, user, user2, user3):
    # verify it's visible in the DB
    comment_item = comment.dynamo.get_comment(comment.id)
    assert comment_item['commentId'] == comment.id

    # comment owner deletes the comment
    comment.delete(deleter_user_id=comment.user_id)

    # verify in-memory item still exists, but not in DB anymore
    assert comment.item['commentId'] == comment.id
    assert comment.dynamo.get_comment(comment.id) is None


def test_forced_delete(comment, comment2, user):
    # verify starting counts
    user.refresh_item()
    assert user.item.get('commentForcedDeletionCount', 0) == 0

    # normal delete one of them, force delete the other
    comment.delete(forced=False)
    comment2.delete(forced=True)

    # verify final counts
    user.refresh_item()
    assert user.item.get('commentForcedDeletionCount', 0) == 1


def test_only_post_owner_and_comment_owner_can_delete_a_comment(
    post_manager, comment_manager, user, user2, user3
):
    post = post_manager.add_post(user, 'pid2', PostType.TEXT_ONLY, text='go go')
    comment1 = comment_manager.add_comment('cid1', post.id, user2.id, 'run far')
    comment2 = comment_manager.add_comment('cid2', post.id, user2.id, 'run far')

    # verify user3 (a rando) cannot delete either of the comments
    with pytest.raises(CommentException, match='not authorized to delete'):
        comment1.delete(deleter_user_id=user3.id)
    with pytest.raises(CommentException, match='not authorized to delete'):
        comment2.delete(deleter_user_id=user3.id)

    assert comment1.refresh_item().item
    assert comment2.refresh_item().item

    # verify post owner can delete a comment that another user left on their post, does not reigster as new activity
    comment1.delete(deleter_user_id=user.id)
    assert comment1.refresh_item().item is None

    # verify comment owner can delete their own comment, does register as new activity
    comment2.delete(deleter_user_id=user2.id)
    assert comment2.refresh_item().item is None
