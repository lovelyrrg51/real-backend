import logging
from unittest.mock import call, patch
from uuid import uuid4

import pendulum
import pytest

from app.models.like.enums import LikeStatus
from app.models.post.enums import PostStatus, PostType
from app.utils import GqlNotificationType


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user


@pytest.fixture
def post(post_manager, user):
    yield post_manager.add_post(user, str(uuid4()), PostType.TEXT_ONLY, text='go go')


@pytest.fixture
def flag_item(post, user2):
    post.flag(user2)
    yield post.flag_dynamo.get(post.id, user2.id)


@pytest.fixture
def like_onymous(post, user, like_manager):
    like_manager.like_post(user, post, LikeStatus.ONYMOUSLY_LIKED)
    yield like_manager.get_like(user.id, post.id)


@pytest.fixture
def like_anonymous(post, user2, like_manager):
    like_manager.like_post(user2, post, LikeStatus.ANONYMOUSLY_LIKED)
    yield like_manager.get_like(user2.id, post.id)


def test_on_flag_add(post_manager, post, user2, flag_item):
    # check starting state
    assert post.refresh_item().item.get('flagCount', 0) == 0

    # postprocess, verify flagCount is incremented & not force achived
    post_manager.on_flag_add(post.id, new_item=flag_item)
    assert post.refresh_item().item.get('flagCount', 0) == 1
    assert post.status != PostStatus.ARCHIVED


def test_on_flag_add_force_archive_by_admin(post_manager, post, user2, caplog, flag_item):
    # check starting state
    assert post.refresh_item().item.get('flagCount', 0) == 0

    # postprocess, verify flagCount is incremented and force archived
    with patch.object(post_manager, 'flag_admin_usernames', ('real', user2.username)):
        with caplog.at_level(logging.WARNING):
            post_manager.on_flag_add(post.id, new_item=flag_item)
    assert len(caplog.records) == 1
    assert 'Force archiving post' in caplog.records[0].msg
    assert post.refresh_item().item.get('flagCount', 0) == 1
    assert post.status == PostStatus.ARCHIVED


def test_on_flag_add_force_archive_by_crowdsourced_criteria(post_manager, post, user2, caplog, flag_item):
    # configure and check starting state
    assert post.refresh_item().item.get('flagCount', 0) == 0
    for _ in range(6):
        post.dynamo.increment_viewed_by_count(post.id)

    # postprocess, verify flagCount is incremented and force archived
    with caplog.at_level(logging.WARNING):
        post_manager.on_flag_add(post.id, new_item=flag_item)
    assert len(caplog.records) == 1
    assert 'Force archiving post' in caplog.records[0].msg
    assert post.refresh_item().item.get('flagCount', 0) == 1
    assert post.status == PostStatus.ARCHIVED


def test_on_like_add(post_manager, post, like_onymous, like_anonymous):
    # check starting state
    post.refresh_item()
    assert post.item.get('onymousLikeCount', 0) == 0
    assert post.item.get('anonymousLikeCount', 0) == 0

    # trigger, check state
    post_manager.on_like_add(post.id, like_onymous.item)
    post.refresh_item()
    assert post.item.get('onymousLikeCount', 0) == 1
    assert post.item.get('anonymousLikeCount', 0) == 0

    # trigger, check state
    post_manager.on_like_add(post.id, like_anonymous.item)
    post.refresh_item()
    assert post.item.get('onymousLikeCount', 0) == 1
    assert post.item.get('anonymousLikeCount', 0) == 1

    # trigger, check state
    post_manager.on_like_add(post.id, like_anonymous.item)
    post.refresh_item()
    assert post.item.get('onymousLikeCount', 0) == 1
    assert post.item.get('anonymousLikeCount', 0) == 2

    # checking junk like status
    with pytest.raises(Exception, match='junkjunk'):
        post_manager.on_like_add(post.id, {**like_onymous.item, 'likeStatus': 'junkjunk'})
    post.refresh_item()
    assert post.item.get('onymousLikeCount', 0) == 1
    assert post.item.get('anonymousLikeCount', 0) == 2


def test_on_like_delete(post_manager, post, like_onymous, like_anonymous, caplog):
    # configure and check starting state
    post_manager.dynamo.increment_onymous_like_count(post.id)
    post_manager.dynamo.increment_anonymous_like_count(post.id)
    post.refresh_item()
    assert post.item.get('onymousLikeCount', 0) == 1
    assert post.item.get('anonymousLikeCount', 0) == 1

    # trigger, check state
    post_manager.on_like_delete(post.id, like_onymous.item)
    post.refresh_item()
    assert post.item.get('onymousLikeCount', 0) == 0
    assert post.item.get('anonymousLikeCount', 0) == 1

    # trigger, check state
    post_manager.on_like_delete(post.id, like_anonymous.item)
    post.refresh_item()
    assert post.item.get('onymousLikeCount', 0) == 0
    assert post.item.get('anonymousLikeCount', 0) == 0

    # trigger, check fails softly
    with caplog.at_level(logging.WARNING):
        post_manager.on_like_delete(post.id, like_onymous.item)
    assert len(caplog.records) == 1
    assert 'Failed to decrement' in caplog.records[0].msg
    assert 'onymousLikeCount' in caplog.records[0].msg
    assert post.id in caplog.records[0].msg
    post.refresh_item()
    assert post.item.get('onymousLikeCount', 0) == 0
    assert post.item.get('anonymousLikeCount', 0) == 0

    # checking junk like status
    with pytest.raises(Exception, match='junkjunk'):
        post_manager.on_like_delete(post.id, {**like_onymous.item, 'likeStatus': 'junkjunk'})
    post.refresh_item()
    assert post.item.get('onymousLikeCount', 0) == 0
    assert post.item.get('anonymousLikeCount', 0) == 0


def test_on_post_view_count_change_update_counts_view_by_post_owner_clears_unviewed_comments(post_manager, post):
    # add some state to clear, verify
    post_manager.dynamo.set_last_unviewed_comment_at(post.item, pendulum.now('utc'))
    post_manager.dynamo.increment_comment_count(post.id, viewed=False)
    post.refresh_item()
    assert 'gsiA3PartitionKey' in post.item
    assert post.item.get('commentsUnviewedCount', 0) == 1

    # react to a view by a non-post owner, verify doesn't change state
    new_item = old_item = {'sortKey': f'view/{uuid4()}'}
    post_manager.on_post_view_count_change_update_counts(post.id, new_item=new_item, old_item=old_item)
    post.refresh_item()
    assert 'gsiA3PartitionKey' in post.item
    assert post.item.get('commentsUnviewedCount', 0) == 1

    # react to the viewCount going down by post owner, verify doesn't change state
    new_item = {'sortKey': f'view/{post.user_id}', 'viewCount': 2}
    old_item = {'sortKey': f'view/{post.user_id}', 'viewCount': 3}
    post_manager.on_post_view_count_change_update_counts(post.id, new_item=new_item, old_item=old_item)
    post.refresh_item()
    assert 'gsiA3PartitionKey' in post.item
    assert post.item.get('commentsUnviewedCount', 0) == 1

    # react to a view by post owner, verify state reset
    new_item = {'sortKey': f'view/{post.user_id}', 'viewCount': 3}
    old_item = {'sortKey': f'view/{post.user_id}', 'viewCount': 2}
    post_manager.on_post_view_count_change_update_counts(post.id, new_item=new_item, old_item=old_item)
    post.refresh_item()
    assert 'gsiA3PartitionKey' not in post.item
    assert post.item.get('commentsUnviewedCount', 0) == 0


def test_on_post_view_count_change_update_counts_view_by_post_owner_race_condition(post_manager, post):
    # delete the post from the DB, verify it's gone
    post.delete()
    assert post_manager.get_post(post.id) is None

    # react to a view by post owner, with the manager mocked so the handler
    # thinks the post exists in the DB up until when the writes fail
    new_item = {'sortKey': f'view/{post.user_id}', 'viewCount': 1}
    with patch.object(post_manager, 'get_post', return_value=post):
        # should not throw exception
        post_manager.on_post_view_count_change_update_counts(post.id, new_item=new_item)


def test_on_comment_add(post_manager, post, user, user2, comment_manager):
    # verify starting state
    post.refresh_item()
    assert 'commentCount' not in post.item
    assert 'commentsUnviewedCount' not in post.item
    assert 'gsiA3PartitionKey' not in post.item
    assert 'gsiA3SortKey' not in post.item

    # postprocess a comment by the owner, which is already viewed
    comment = comment_manager.add_comment(str(uuid4()), post.id, user.id, 'lore')
    post_manager.on_comment_add(comment.id, comment.item)
    post.refresh_item()
    assert post.item['commentCount'] == 1
    assert 'commentsUnviewedCount' not in post.item
    assert 'gsiA3PartitionKey' not in post.item
    assert 'gsiA3SortKey' not in post.item

    # postprocess a comment by other, which has not yet been viewed
    now = pendulum.now('utc')
    comment = comment_manager.add_comment(str(uuid4()), post.id, user2.id, 'lore', now=now)
    post_manager.on_comment_add(comment.id, comment.item)
    post.refresh_item()
    assert post.item['commentCount'] == 2
    assert post.item['commentsUnviewedCount'] == 1
    assert post.item['gsiA3PartitionKey'].split('/') == ['post', user.id]
    assert pendulum.parse(post.item['gsiA3SortKey']) == now

    # postprocess another comment by other, which has not yet been viewed
    now = pendulum.now('utc')
    comment = comment_manager.add_comment(str(uuid4()), post.id, user2.id, 'lore', now=now)
    post_manager.on_comment_add(comment.id, comment.item)
    post.refresh_item()
    assert post.item['commentCount'] == 3
    assert post.item['commentsUnviewedCount'] == 2
    assert post.item['gsiA3PartitionKey'].split('/') == ['post', user.id]
    assert pendulum.parse(post.item['gsiA3SortKey']) == now


def test_on_comment_delete(post_manager, post, user2, caplog, comment_manager):
    # configure starting state, verify
    post_manager.dynamo.increment_comment_count(post.id, viewed=False)
    post_manager.dynamo.increment_comment_count(post.id, viewed=False)
    post.refresh_item()
    assert post.item['commentCount'] == 2
    assert post.item['commentsUnviewedCount'] == 2

    # postprocess a deleted comment, verify counts drop as expected
    comment = comment_manager.add_comment(str(uuid4()), post.id, user2.id, 'lore')
    post_manager.on_comment_delete(comment.id, comment.item)
    post.refresh_item()
    assert post.item['commentCount'] == 1
    assert post.item['commentsUnviewedCount'] == 1

    # postprocess a deleted comment, verify counts drop as expected
    post_manager.on_comment_delete(comment.id, comment.item)
    post.refresh_item()
    assert post.item['commentCount'] == 0
    assert post.item['commentsUnviewedCount'] == 0

    # postprocess a deleted comment, verify fails softly and final state
    with caplog.at_level(logging.WARNING):
        post_manager.on_comment_delete(comment.id, comment.item)
    assert len(caplog.records) == 2
    assert 'Failed to decrement commentCount' in caplog.records[0].msg
    assert 'Failed to decrement commentsUnviewedCount' in caplog.records[1].msg
    assert post.id in caplog.records[0].msg
    assert post.id in caplog.records[1].msg
    post.refresh_item()
    assert post.item['commentCount'] == 0
    assert post.item['commentsUnviewedCount'] == 0


def test_comment_deleted_with_post_views(post_manager, post, user, user2, caplog, comment_manager):
    # post owner adds a acomment
    comment1 = comment_manager.add_comment(str(uuid4()), post.id, user.id, 'lore ipsum')
    post_manager.on_comment_add(comment1.id, comment1.item)

    # other user adds a comment
    comment2 = comment_manager.add_comment(str(uuid4()), post.id, user2.id, 'lore ipsum')
    post_manager.on_comment_add(comment2.id, comment2.item)

    # post owner views all the comments
    post_manager.record_views([post.id], user.id)
    post_manager.on_post_view_count_change_update_counts(post.id, {'sortKey': f'view/{user.id}', 'viewCount': 1})

    # other user adds another comment
    comment3 = comment_manager.add_comment(str(uuid4()), post.id, user2.id, 'lore ipsum')
    post_manager.on_comment_add(comment3.id, comment3.item)

    # other user adds another comment
    comment4 = comment_manager.add_comment(str(uuid4()), post.id, user2.id, 'lore ipsum')
    post_manager.on_comment_add(comment4.id, comment4.item)

    # verify starting state
    post.refresh_item()
    assert post.item['commentCount'] == 4
    assert post.item['commentsUnviewedCount'] == 2
    assert pendulum.parse(post.item['gsiA3SortKey']) == comment4.created_at

    # postprocess deleteing comment4, verify state
    post_manager.on_comment_delete(comment4.id, comment4.item)
    post.refresh_item()
    assert post.item['commentCount'] == 3
    assert post.item['commentsUnviewedCount'] == 1
    assert pendulum.parse(post.item['gsiA3SortKey']) == comment4.created_at

    # postprocess deleteing comment2, verify state
    post_manager.on_comment_delete(comment2.id, comment2.item)
    post.refresh_item()
    assert post.item['commentCount'] == 2
    assert post.item['commentsUnviewedCount'] == 1
    assert pendulum.parse(post.item['gsiA3SortKey']) == comment4.created_at

    # postprocess deleteing comment4, verify state
    post_manager.on_comment_delete(comment4.id, comment4.item)
    post.refresh_item()
    assert post.item['commentCount'] == 1
    assert post.item['commentsUnviewedCount'] == 0
    assert 'gsiA3SortKey' not in post.item

    # postprocess deleteing comment1, verify state
    post_manager.on_comment_delete(comment1.id, comment1.item)
    post.refresh_item()
    assert post.item['commentCount'] == 0
    assert post.item['commentsUnviewedCount'] == 0
    assert 'gsiA3SortKey' not in post.item


def test_on_album_delete_remove_posts(post_manager, album_manager, user):
    # create two albums, put a post in 2nd, verify
    album1 = album_manager.add_album(user.id, str(uuid4()), 'a1')
    album2 = album_manager.add_album(user.id, str(uuid4()), 'a1')
    post21 = post_manager.add_post(user, str(uuid4()), PostType.TEXT_ONLY, text='hey!', album_id=album2.id)
    assert post21.refresh_item().item['albumId'] == album2.id

    # trigger for 1st album, verify no change
    post_manager.on_album_delete_remove_posts(album1.id, old_item=album1.item)
    assert post21.refresh_item().item['albumId'] == album2.id

    # add two posts to 1st album, verify
    post11 = post_manager.add_post(user, str(uuid4()), PostType.TEXT_ONLY, text='hey!', album_id=album1.id)
    post12 = post_manager.add_post(user, str(uuid4()), PostType.TEXT_ONLY, text='hey!', album_id=album1.id)
    assert post11.refresh_item().item['albumId'] == album1.id
    assert post12.refresh_item().item['albumId'] == album1.id

    # trigger for 1st album, verify those two posts removed from album
    post_manager.on_album_delete_remove_posts(album1.id, old_item=album1.item)
    assert 'albumId' not in post11.refresh_item().item
    assert 'albumId' not in post12.refresh_item().item
    assert post21.refresh_item().item['albumId'] == album2.id

    # trigger for the 2nd album, verify last post removed from album
    post_manager.on_album_delete_remove_posts(album2.id, old_item=album2.item)
    assert 'albumId' not in post11.refresh_item().item
    assert 'albumId' not in post12.refresh_item().item
    assert 'albumId' not in post21.refresh_item().item


def test_on_post_status_change_fire_gql_notifications(post_manager, post, user):
    # transition from PENDING to PROCESSING, should not fire
    old_item = {**post.item, 'postStatus': PostStatus.PENDING}
    new_item = {**post.item, 'postStatus': PostStatus.PROCESSING}
    with patch.object(post_manager, 'appsync') as appsync_mock:
        post_manager.on_post_status_change_fire_gql_notifications(post.id, new_item=new_item, old_item=old_item)
    assert appsync_mock.mock_calls == []

    # transition from PENDING to COMPLETED, should fire for completed
    old_item = {**post.item, 'postStatus': PostStatus.PENDING}
    new_item = {**post.item, 'postStatus': PostStatus.COMPLETED}
    with patch.object(post_manager, 'appsync') as appsync_mock:
        post_manager.on_post_status_change_fire_gql_notifications(post.id, new_item=new_item, old_item=old_item)
    assert appsync_mock.mock_calls == [
        call.client.fire_notification(user.id, GqlNotificationType.POST_COMPLETED, postId=post.id)
    ]

    # transition from PROCESSING to COMPLETED, should fire for completed
    old_item = {**post.item, 'postStatus': PostStatus.PROCESSING}
    new_item = {**post.item, 'postStatus': PostStatus.COMPLETED}
    with patch.object(post_manager, 'appsync') as appsync_mock:
        post_manager.on_post_status_change_fire_gql_notifications(post.id, new_item=new_item, old_item=old_item)
    assert appsync_mock.mock_calls == [
        call.client.fire_notification(user.id, GqlNotificationType.POST_COMPLETED, postId=post.id)
    ]

    # transition from COMPLETED to ARCHIVED, should not fire
    old_item = {**post.item, 'postStatus': PostStatus.COMPLETED}
    new_item = {**post.item, 'postStatus': PostStatus.ARCHIVED}
    with patch.object(post_manager, 'appsync') as appsync_mock:
        post_manager.on_post_status_change_fire_gql_notifications(post.id, new_item=new_item, old_item=old_item)
    assert appsync_mock.mock_calls == []

    # transition from ARCHIVED to COMPLETED, should not fire
    old_item = {**post.item, 'postStatus': PostStatus.ARCHIVED}
    new_item = {**post.item, 'postStatus': PostStatus.COMPLETED}
    with patch.object(post_manager, 'appsync') as appsync_mock:
        post_manager.on_post_status_change_fire_gql_notifications(post.id, new_item=new_item, old_item=old_item)
    assert appsync_mock.mock_calls == []

    # transition from anything to ERROR, should fire for error
    old_item = {**post.item, 'postStatus': 'anything'}
    new_item = {**post.item, 'postStatus': PostStatus.ERROR}
    with patch.object(post_manager, 'appsync') as appsync_mock:
        post_manager.on_post_status_change_fire_gql_notifications(post.id, new_item=new_item, old_item=old_item)
    assert appsync_mock.mock_calls == [
        call.client.fire_notification(user.id, GqlNotificationType.POST_ERROR, postId=post.id)
    ]


@pytest.mark.parametrize('is_verified', [True, False])
def test_on_post_verification_hidden_change_update_is_verified(post_manager, post, user, is_verified):
    # check starting state
    assert 'isVerified' not in post.item
    assert 'isVerifiedHiddenValue' not in post.item
    assert 'verificationHidden' not in post.item

    # verificationHidden is set before the post is verified, check
    new_item = {**post.item, 'verificationHidden': True}
    for old_item in [{**post.item}, {**post.item, 'verificationHidden': False}, None]:
        post_manager.on_post_verification_hidden_change_update_is_verified(
            post.id, new_item=new_item, old_item=old_item
        )
        post.refresh_item()
        assert 'isVerified' not in post.item
        assert 'isVerifiedHiddenValue' not in post.item

    # verificationHidden is set as True, check
    new_item = {**post.item, 'verificationHidden': True, 'isVerified': is_verified}
    for old_item in [{**post.item}, {**post.item, 'verificationHidden': False}, None]:
        post_manager.on_post_verification_hidden_change_update_is_verified(
            post.id, new_item=new_item, old_item=old_item
        )
        post.refresh_item()
        assert post.item['isVerified'] is True
        assert post.item['isVerifiedHiddenValue'] is is_verified

    # verificationHidden is set as False, check
    old_item = {**post.item, 'verificationHidden': True}
    for new_item in [
        {**post.item, 'verificationHidden': False, 'isVerified': is_verified},
        {**post.item, 'isVerified': is_verified},
    ]:
        post_manager.on_post_verification_hidden_change_update_is_verified(
            post.id, new_item=new_item, old_item=old_item
        )
        post.refresh_item()
        assert post.item['isVerified'] is is_verified
        assert 'isVerifiedHiddenValue' not in post.item


def test_on_user_delete_delete_all_by_user(post_manager, user):
    assert list(post_manager.dynamo.generate_posts_by_user(user.id)) == []

    # test delete none
    post_manager.on_user_delete_delete_all_by_user(user.id, old_item=user.item)
    assert list(post_manager.dynamo.generate_posts_by_user(user.id)) == []

    # user adds two posts
    post1 = post_manager.add_post(user, 'pid1', PostType.TEXT_ONLY, text='t')
    post2 = post_manager.add_post(user, 'pid2', PostType.TEXT_ONLY, text='t')
    post_items = list(post_manager.dynamo.generate_posts_by_user(user.id))
    assert len(post_items) == 2
    assert post_items[0]['postId'] == post1.id
    assert post_items[1]['postId'] == post2.id

    # test delete those posts
    post_manager.on_user_delete_delete_all_by_user(user.id, old_item=user.item)
    assert list(post_manager.dynamo.generate_posts_by_user(user.id)) == []


def test_on_post_view_add_delete_sync_viewed_by_counts(post_manager, post, caplog):
    assert 'viewedByCount' not in post.refresh_item().item
    assert 'postViewedByCount' not in post.user.refresh_item().item
    item_post_owner = {'sortKey': f'view/{post.user_id}'}
    item_other_user = {'sortKey': f'view/{uuid4()}'}

    # trigger for creation of a new post view, verify
    post_manager.on_post_view_add_delete_sync_viewed_by_counts(post.id, new_item=item_other_user)
    assert post.refresh_item().item['viewedByCount'] == 1
    assert post.user.refresh_item().item['postViewedByCount'] == 1

    # trigger for creation of a new post view by post owner, verify does not affect counts
    post_manager.on_post_view_add_delete_sync_viewed_by_counts(post.id, new_item=item_post_owner)
    assert post.refresh_item().item['viewedByCount'] == 1
    assert post.user.refresh_item().item['postViewedByCount'] == 1

    # trigger for deletion of a post view by post owner, verify does not affect counts
    post_manager.on_post_view_add_delete_sync_viewed_by_counts(post.id, old_item=item_post_owner)
    assert post.refresh_item().item['viewedByCount'] == 1
    assert post.user.refresh_item().item['postViewedByCount'] == 1

    # trigger for deletion of post view, verify
    post_manager.on_post_view_add_delete_sync_viewed_by_counts(post.id, old_item=item_other_user)
    assert post.refresh_item().item['viewedByCount'] == 0
    assert post.user.refresh_item().item['postViewedByCount'] == 0

    # trigger for deletion of post view, verify logs error doesn't crash
    with caplog.at_level(logging.WARNING):
        post_manager.on_post_view_add_delete_sync_viewed_by_counts(post.id, old_item=item_other_user)
    assert len(caplog.records) == 2
    assert all(x in caplog.records[0].msg for x in ('Failed to decrement viewedByCount', post.id))
    assert all(x in caplog.records[1].msg for x in ('Failed to decrement postViewedByCount', post.user_id))
    assert post.refresh_item().item['viewedByCount'] == 0
    assert post.user.refresh_item().item['postViewedByCount'] == 0


def test_on_post_delete(post_manager, post):
    with patch.object(post_manager, 'elasticsearch_client') as elasticsearch_client_mock:
        post_manager.on_post_delete(post.id, post.refresh_item().item)
    assert elasticsearch_client_mock.mock_calls == [call.delete_post(post.id)]


def test_sync_elasticsearch(post_manager, post):
    with patch.object(post_manager, 'elasticsearch_client') as elasticsearch_client_mock:
        post_manager.sync_elasticsearch(post.id, {'keywords': ['spock']})
    assert elasticsearch_client_mock.mock_calls == [
        call.put_post(post.id, ['spock']),
        call.put_keyword(post.id, 'spock'),
    ]
