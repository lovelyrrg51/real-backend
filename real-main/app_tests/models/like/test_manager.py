import uuid

import pytest

from app.models.follower.enums import FollowStatus
from app.models.like.enums import LikeStatus
from app.models.like.exceptions import AlreadyLiked, LikeException
from app.models.post.enums import PostType
from app.models.user.enums import UserPrivacyStatus


@pytest.fixture
def user1(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user1


@pytest.fixture
def user1_posts(post_manager, user1):
    post1 = post_manager.add_post(user1, 'pid1', PostType.TEXT_ONLY, text='lore ipsum')
    post2 = post_manager.add_post(user1, 'pid2', PostType.TEXT_ONLY, text='lore ipsum')
    yield (post1, post2)


@pytest.fixture
def user2_posts(post_manager, user2):
    post1 = post_manager.add_post(user2, 'pid3', PostType.TEXT_ONLY, text='lore ipsum')
    post2 = post_manager.add_post(user2, 'pid4', PostType.TEXT_ONLY, text='lore ipsum')
    yield (post1, post2)


def test_like_post(like_manager, user1, user2, user2_posts):
    # verify initial state
    post, _ = user2_posts
    assert like_manager.get_like(user1.id, post.id) is None
    assert like_manager.get_like(user2.id, post.id) is None

    # like post, verify like exists
    like_manager.like_post(user1, post, LikeStatus.ANONYMOUSLY_LIKED)
    like = like_manager.get_like(user1.id, post.id)
    assert like.item['likeStatus'] == LikeStatus.ANONYMOUSLY_LIKED

    # like post the other way, verify like exists
    like_manager.like_post(user2, post, LikeStatus.ONYMOUSLY_LIKED)
    like = like_manager.get_like(user2.id, post.id)
    assert like.item['likeStatus'] == LikeStatus.ONYMOUSLY_LIKED


def test_cant_like_post_blocked(like_manager, block_manager, user1, user2, user1_posts, user2_posts):
    # user1 blocks user2
    assert block_manager.block(user1, user2)

    # user2 can't like user1's posts
    with pytest.raises(LikeException):
        like_manager.like_post(user1, user2_posts[0], LikeStatus.ANONYMOUSLY_LIKED)

    # and user1 can't like user2's posts
    with pytest.raises(LikeException):
        like_manager.like_post(user2, user1_posts[0], LikeStatus.ANONYMOUSLY_LIKED)


def test_cant_like_post_of_private_user_without_following(
    like_manager, follower_manager, user1, user1_posts, user2
):
    post = user1_posts[0]
    # user 1 goes private
    user1.set_privacy_status(UserPrivacyStatus.PRIVATE)

    # user 2 can't like their post
    with pytest.raises(LikeException):
        like_manager.like_post(user2, post, LikeStatus.ANONYMOUSLY_LIKED)

    # user 2 requests to follow
    follower_manager.request_to_follow(user2, user1)

    # user 2 still can't like their post
    with pytest.raises(LikeException):
        like_manager.like_post(user2, post, LikeStatus.ANONYMOUSLY_LIKED)

    # user 1 first denies the follow request
    follower_manager.get_follow(user2.id, user1.id).deny()

    # user 2 still can't like their post
    with pytest.raises(LikeException):
        like_manager.like_post(user2, post, LikeStatus.ANONYMOUSLY_LIKED)

    # user 1 now accepts the follow request
    follower_manager.get_follow(user2.id, user1.id).accept()

    # user 2 can now like the post
    like_manager.like_post(user2, post, LikeStatus.ANONYMOUSLY_LIKED)
    like = like_manager.get_like(user2.id, post.id)
    assert like.item['likeStatus'] == LikeStatus.ANONYMOUSLY_LIKED


def test_cant_like_incomplete_post(like_manager, post_manager, user1, user2, grant_data_b64):
    # add a image post which will be left in a pending state
    post = post_manager.add_post(user1, 'pid1', PostType.IMAGE)

    # verify we can't like it
    with pytest.raises(LikeException):
        like_manager.like_post(user2, post, LikeStatus.ONYMOUSLY_LIKED)

    # complete the post
    post.process_image_upload(image_data=grant_data_b64)

    # now we should be able to like it
    like_manager.like_post(user2, post, LikeStatus.ONYMOUSLY_LIKED)
    like = like_manager.get_like(user2.id, post.id)
    assert like.item['likeStatus'] == LikeStatus.ONYMOUSLY_LIKED


def test_cant_like_post_likes_disabled(like_manager, post_manager, user1, user2):
    # add a post with likes disabled
    post = post_manager.add_post(user1, 'pid1', PostType.TEXT_ONLY, text='t', likes_disabled=True)

    # verify we can't like it
    with pytest.raises(LikeException):
        like_manager.like_post(user2, post, LikeStatus.ONYMOUSLY_LIKED)

    # enable likes on the post, post owner disables likes
    post.set(likes_disabled=False)
    user1.update_details(likes_disabled=True)

    # verify we can't like it
    with pytest.raises(LikeException):
        like_manager.like_post(user2, post, LikeStatus.ONYMOUSLY_LIKED)

    # post owner enables likes, we disable likes
    user1.update_details(likes_disabled=False)
    user2.update_details(likes_disabled=True)

    # verify we can't like it
    with pytest.raises(LikeException):
        like_manager.like_post(user2, post, LikeStatus.ONYMOUSLY_LIKED)

    # we enable likes
    user2.update_details(likes_disabled=False)

    # verify we can like it
    like_manager.like_post(user2, post, LikeStatus.ONYMOUSLY_LIKED)
    like = like_manager.get_like(user2.id, post.id)
    assert like.item['likeStatus'] == LikeStatus.ONYMOUSLY_LIKED


def test_like_post_failure_post_already_liked(like_manager, user1, user2_posts):
    post, _ = user2_posts

    # add a post and like it
    like_manager.like_post(user1, post, LikeStatus.ANONYMOUSLY_LIKED)

    # try to like it again with same like status
    with pytest.raises(AlreadyLiked):
        like_manager.like_post(user1, post, LikeStatus.ANONYMOUSLY_LIKED)

    # try to like it again with different like status
    with pytest.raises(AlreadyLiked):
        like_manager.like_post(user1, post, LikeStatus.ONYMOUSLY_LIKED)


def test_dislike_all_of_post(like_manager, user1, user2, user1_posts):
    post1, post2 = user1_posts

    # check likes
    assert list(like_manager.dynamo.generate_of_post(post1.id)) == []
    assert list(like_manager.dynamo.generate_of_post(post2.id)) == []

    # add two likes on one, one on the other
    like_manager.like_post(user1, post1, LikeStatus.ANONYMOUSLY_LIKED)
    like_manager.like_post(user2, post1, LikeStatus.ONYMOUSLY_LIKED)
    like_manager.like_post(user1, post2, LikeStatus.ONYMOUSLY_LIKED)

    # check likes
    like_items = like_manager.dynamo.generate_of_post(post1.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [
        (user1.id, post1.id),
        (user2.id, post1.id),
    ]
    like_items = like_manager.dynamo.generate_of_post(post2.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [(user1.id, post2.id)]

    # clear one post's likes
    like_manager.dislike_all_of_post(post1.id)

    # check likes
    assert list(like_manager.dynamo.generate_of_post(post1.id)) == []
    like_items = like_manager.dynamo.generate_of_post(post2.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [(user1.id, post2.id)]

    # clear the other post's likes
    like_manager.dislike_all_of_post(post2.id)

    # check likes
    assert list(like_manager.dynamo.generate_of_post(post1.id)) == []
    assert list(like_manager.dynamo.generate_of_post(post2.id)) == []


def test_on_user_delete_dislike_all_by_user(like_manager, user1, user2, user1_posts):
    post1, post2 = user1_posts

    # check likes
    assert list(like_manager.dynamo.generate_of_post(post1.id)) == []
    assert list(like_manager.dynamo.generate_of_post(post2.id)) == []

    # add two likes on one, one on the other
    like_manager.like_post(user1, post1, LikeStatus.ANONYMOUSLY_LIKED)
    like_manager.like_post(user2, post1, LikeStatus.ONYMOUSLY_LIKED)
    like_manager.like_post(user1, post2, LikeStatus.ONYMOUSLY_LIKED)

    # check likes
    like_items = like_manager.dynamo.generate_of_post(post1.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [
        (user1.id, post1.id),
        (user2.id, post1.id),
    ]
    like_items = like_manager.dynamo.generate_of_post(post2.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [(user1.id, post2.id)]

    # clear one users likes
    like_manager.on_user_delete_dislike_all_by_user(user1.id, old_item=user1.item)

    # check likes
    like_items = like_manager.dynamo.generate_of_post(post1.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [(user2.id, post1.id)]
    assert list(like_manager.dynamo.generate_of_post(post2.id)) == []

    # clear other users likes
    like_manager.on_user_delete_dislike_all_by_user(user2.id, old_item=user2.item)

    # check likes
    assert list(like_manager.dynamo.generate_of_post(post1.id)) == []
    assert list(like_manager.dynamo.generate_of_post(post2.id)) == []


def test_dislike_all_by_user_from_user(like_manager, user1, user2, user1_posts, user2_posts):
    post1, _ = user1_posts
    post2, _ = user2_posts

    # check likes
    assert list(like_manager.dynamo.generate_of_post(post1.id)) == []
    assert list(like_manager.dynamo.generate_of_post(post2.id)) == []

    # add two likes on one, one on the other
    like_manager.like_post(user1, post1, LikeStatus.ANONYMOUSLY_LIKED)
    like_manager.like_post(user2, post1, LikeStatus.ONYMOUSLY_LIKED)
    like_manager.like_post(user1, post2, LikeStatus.ONYMOUSLY_LIKED)

    # check likes
    like_items = like_manager.dynamo.generate_of_post(post1.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [
        (user1.id, post1.id),
        (user2.id, post1.id),
    ]
    like_items = like_manager.dynamo.generate_of_post(post2.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [(user1.id, post2.id)]

    # clear one users likes of another's post, does nothing
    like_manager.dislike_all_by_user_from_user(user2.id, post2.user_id)

    # check likes
    like_items = like_manager.dynamo.generate_of_post(post1.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [
        (user1.id, post1.id),
        (user2.id, post1.id),
    ]
    like_items = like_manager.dynamo.generate_of_post(post2.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [(user1.id, post2.id)]

    # clear another combo of liked_by and posted_by
    like_manager.dislike_all_by_user_from_user(user2.id, post1.user_id)

    # check likes
    like_items = like_manager.dynamo.generate_of_post(post1.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [(user1.id, post1.id)]
    like_items = like_manager.dynamo.generate_of_post(post2.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [(user1.id, post2.id)]

    # clear another combo of liked_by and posted_by
    like_manager.dislike_all_by_user_from_user(user1.id, post1.user_id)

    # check likes
    assert list(like_manager.dynamo.generate_of_post(post1.id)) == []
    like_items = like_manager.dynamo.generate_of_post(post2.id)
    assert [(li['likedByUserId'], li['postId']) for li in like_items] == [(user1.id, post2.id)]

    # clear another combo of liked_by and posted_by
    like_manager.dislike_all_by_user_from_user(user1.id, post2.user_id)

    # check likes
    assert list(like_manager.dynamo.generate_of_post(post1.id)) == []
    assert list(like_manager.dynamo.generate_of_post(post2.id)) == []


def test_on_user_follow_status_change_sync_likes(like_manager, follower_manager, user1, user2, user1_posts):
    # user2 likes user1's post, verify
    post1, _ = user1_posts
    like_manager.like_post(user2, post1, LikeStatus.ANONYMOUSLY_LIKED)
    assert like_manager.get_like(user2.id, post1.id)

    # user1 goes private
    user1.set_privacy_status(UserPrivacyStatus.PRIVATE)
    assert user1.refresh_item().item['privacyStatus'] == UserPrivacyStatus.PRIVATE

    # user2 requests to follow user1, verify no change to likes
    old_item = None
    new_item = follower_manager.dynamo.add_following(user2.id, user1.id, FollowStatus.REQUESTED)
    like_manager.on_user_follow_status_change_sync_likes(user1.id, new_item=new_item, old_item=old_item)
    assert like_manager.get_like(user2.id, post1.id)

    # user1 denies the follow request. Verify the like is deleted
    old_item = new_item
    new_item = follower_manager.dynamo.update_following_status(old_item, FollowStatus.DENIED)
    like_manager.on_user_follow_status_change_sync_likes(user1.id, new_item=new_item, old_item=old_item)
    assert like_manager.get_like(user2.id, post1.id) is None

    # user1 accepts the follow request
    old_item = new_item
    new_item = follower_manager.dynamo.update_following_status(old_item, FollowStatus.FOLLOWING)
    like_manager.on_user_follow_status_change_sync_likes(user1.id, new_item=new_item, old_item=old_item)
    assert like_manager.get_like(user2.id, post1.id) is None

    # user2 re-likes the post
    like_manager.like_post(user2, post1, LikeStatus.ONYMOUSLY_LIKED)
    assert like_manager.get_like(user2.id, post1.id)

    # follow item is deleted, verify the like is deleted
    old_item = new_item
    like_manager.on_user_follow_status_change_sync_likes(user1.id, old_item=old_item)
    assert like_manager.get_like(user2.id, post1.id) is None
