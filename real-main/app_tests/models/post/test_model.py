import decimal
import logging
import uuid
from os import path
from unittest import mock

import pendulum
import pytest

from app.mixins.view.enums import ViewType
from app.models.post.enums import PostStatus, PostType
from app.models.post.exceptions import PostException
from app.models.post.model import Post
from app.models.user.enums import UserSubscriptionLevel
from app.utils import image_size

grant_height = 320
grant_width = 240
grant_path = path.join(path.dirname(__file__), '..', '..', 'fixtures', 'grant.jpg')
blank_path = path.join(path.dirname(__file__), '..', '..', 'fixtures', 'big-blank.jpg')

heic_path = path.join(path.dirname(__file__), '..', '..', 'fixtures', 'IMG_0265.HEIC')
heic_width = 4032
heic_height = 3024

grant_colors = [
    {'r': 51, 'g': 58, 'b': 45},
    {'r': 186, 'g': 206, 'b': 228},
    {'r': 145, 'g': 154, 'b': 169},
    {'r': 158, 'g': 180, 'b': 205},
    {'r': 130, 'g': 123, 'b': 125},
]


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user


@pytest.fixture
def real_user(user_manager, cognito_client):
    user_id = str(uuid.uuid4())
    cognito_client.create_user_pool_entry(user_id, 'real', verified_email='real-test@real.app')
    yield user_manager.create_cognito_only_user(user_id, 'real')


@pytest.fixture
def post(post_manager, user):
    yield post_manager.add_post(user, 'pid1', PostType.TEXT_ONLY, text='t')


@pytest.fixture
def pending_video_post(post_manager, user2):
    yield post_manager.add_post(user2, 'pidv1', PostType.VIDEO)


@pytest.fixture
def pending_image_post(post_manager, user2):
    yield post_manager.add_post(user2, 'pidi1', PostType.IMAGE)


@pytest.fixture
def pending_image_post_heic(post_manager, user2):
    yield post_manager.add_post(user2, 'pid2', PostType.IMAGE, image_input={'imageFormat': 'HEIC'})


@pytest.fixture
def image_post(user, post_manager, grant_data_b64):
    yield post_manager.add_post(
        user, str(uuid.uuid4()), PostType.IMAGE, image_input={'imageData': grant_data_b64}
    )


@pytest.fixture
def processing_video_post(pending_video_post, s3_uploads_client, grant_data):
    post = pending_video_post
    post.item = post.dynamo.set_post_status(post.item, PostStatus.PROCESSING)
    image_path = post.get_image_path(image_size.NATIVE)
    s3_uploads_client.put_object(image_path, grant_data, 'image/jpeg')
    yield post


@pytest.fixture
def albums(album_manager, user2):
    album1 = album_manager.add_album(user2.id, 'aid-1', 'album name')
    album2 = album_manager.add_album(user2.id, 'aid-2', 'album name')
    yield [album1, album2]


@pytest.fixture
def post_with_expiration(post_manager, user2):
    yield post_manager.add_post(
        user2,
        'pid2',
        PostType.TEXT_ONLY,
        text='t',
        lifetime_duration=pendulum.duration(hours=1),
    )


@pytest.fixture
def post_with_media(post_manager, user2, image_data_b64):
    yield post_manager.add_post(user2, 'pid', PostType.IMAGE, image_input={'imageData': image_data_b64}, text='t')


def test_refresh_item(post):
    # go behind their back and edit the post in the DB
    new_post_item = post.dynamo.increment_viewed_by_count(post.id)
    assert new_post_item != post.item

    # now refresh the item, and check they now do match
    post.refresh_item()
    assert new_post_item == post.item


def test_get_original_video_path(post):
    user_id = post.item['postedByUserId']
    post_id = post.id

    video_path = post.get_original_video_path()
    assert video_path == f'{user_id}/post/{post_id}/video-original.mov'


def test_get_video_writeonly_url(cloudfront_client, s3_uploads_client):
    item = {
        'postedByUserId': 'user-id',
        'postId': 'post-id',
        'postType': PostType.VIDEO,
        'postStatus': PostStatus.PENDING,
    }
    expected_url = {}
    cloudfront_client.configure_mock(**{'generate_presigned_url.return_value': expected_url})

    post = Post(item, cloudfront_client=cloudfront_client, s3_uploads_client=s3_uploads_client)
    url = post.get_video_writeonly_url()
    assert url == expected_url

    expected_path = 'user-id/post/post-id/video-original.mov'
    assert cloudfront_client.mock_calls == [mock.call.generate_presigned_url(expected_path, ['PUT'])]


def test_get_image_readonly_url(cloudfront_client, s3_uploads_client):
    item = {
        'postedByUserId': 'user-id',
        'postId': 'post-id',
        'postType': PostType.IMAGE,
        'postStatus': PostStatus.PENDING,
    }
    expected_url = {}
    cloudfront_client.configure_mock(**{'generate_presigned_url.return_value': expected_url})

    post = Post(item, cloudfront_client=cloudfront_client, s3_uploads_client=s3_uploads_client)
    url = post.get_image_readonly_url(image_size.NATIVE)
    assert url == expected_url

    expected_path = f'user-id/post/post-id/image/{image_size.NATIVE.filename}'
    assert cloudfront_client.mock_calls == [mock.call.generate_presigned_url(expected_path, ['GET', 'HEAD'])]


def test_get_hls_access_cookies(cloudfront_client, s3_uploads_client):
    user_id = 'uid'
    post_id = 'pid'
    item = {
        'postedByUserId': user_id,
        'postId': post_id,
        'postType': PostType.VIDEO,
        'postStatus': PostStatus.COMPLETED,
    }
    domain = 'cf-domain'
    expires_at = pendulum.now('utc')
    presigned_cookies = {
        'ExpiresAt': expires_at.to_iso8601_string(),
        'CloudFront-Policy': 'cf-policy',
        'CloudFront-Signature': 'cf-signature',
        'CloudFront-Key-Pair-Id': 'cf-kpid',
    }
    cloudfront_client.configure_mock(
        **{'generate_presigned_cookies.return_value': presigned_cookies, 'domain': domain}
    )

    post = Post(item, cloudfront_client=cloudfront_client, s3_uploads_client=s3_uploads_client)
    access_cookies = post.get_hls_access_cookies()

    assert access_cookies == {
        'domain': domain,
        'path': f'/{user_id}/post/{post_id}/video-hls/',
        'expiresAt': expires_at.to_iso8601_string(),
        'policy': 'cf-policy',
        'signature': 'cf-signature',
        'keyPairId': 'cf-kpid',
    }

    cookie_path = f'{user_id}/post/{post_id}/video-hls/video*'
    assert cloudfront_client.mock_calls == [mock.call.generate_presigned_cookies(cookie_path)]


def test_set_checksum(post):
    assert 'checksum' not in post.item

    # put some content with a known md5 up in s3
    content = b'anything'
    md5 = 'f0e166dc34d14d6c228ffac576c9a43c'
    path = post.get_image_path(image_size.NATIVE)
    post.s3_uploads_client.put_object(path, content, 'application/octet-stream')

    # set the checksum, check what was saved to the DB
    post.set_checksum()
    assert post.item['checksum'] == md5
    post.refresh_item()
    assert post.item['checksum'] == md5


def test_set_is_verified_minimal(pending_image_post):
    # check initial state and configure mock
    post = pending_image_post
    assert 'isVerified' not in post.item
    post.post_verification_client = mock.Mock(**{'verify_image.return_value': True})

    # do the call, check final state
    post.set_is_verified()
    assert post.item == post.refresh_item().item
    assert post.item['isVerified'] is True
    assert 'isVerifiedHiddenValue' not in post.item

    # check mock called correctly
    assert post.post_verification_client.mock_calls == [
        mock.call.verify_image(
            post.get_image_readonly_url(image_size.NATIVE),
            image_format=None,
            original_format=None,
            taken_in_real=None,
        )
    ]


def test_set_is_verified_maximal(pending_image_post):
    # check initial state and configure mock
    post = pending_image_post
    assert 'isVerified' not in post.item
    post.post_verification_client = mock.Mock(**{'verify_image.return_value': False})
    post.image_item['imageFormat'] = 'ii'
    post.image_item['originalFormat'] = 'oo'
    post.image_item['takenInReal'] = False
    post.item['verificationHidden'] = True

    # do the call, check final state
    post.set_is_verified()
    assert post.item == post.refresh_item().item
    assert post.item['isVerified'] is True
    assert post.item['isVerifiedHiddenValue'] is False

    # check mock called correctly
    assert post.post_verification_client.mock_calls == [
        mock.call.verify_image(
            post.get_image_readonly_url(image_size.NATIVE),
            image_format='ii',
            original_format='oo',
            taken_in_real=False,
        )
    ]


def test_set_expires_at(post):
    # add a post without an expires at
    assert 'expiresAt' not in post.item

    # set the expires at to something
    now = pendulum.now('utc')
    post.follower_manager = mock.Mock(post.follower_manager)
    post.set_expires_at(now)
    assert post.item['expiresAt'] == now.to_iso8601_string()

    # make sure that stuck in db
    post.refresh_item()
    assert post.item['expiresAt'] == now.to_iso8601_string()

    # check that the follower_manager was called correctly
    assert post.follower_manager.mock_calls == [
        mock.call.refresh_first_story(story_prev=None, story_now=post.item)
    ]

    # set the expires at to something else
    post.follower_manager.reset_mock()
    now = pendulum.now('utc')
    post_org_item = post.item.copy()
    post.set_expires_at(now)
    assert post.item['expiresAt'] == now.to_iso8601_string()

    # make sure that stuck in db
    post.refresh_item()
    assert post.item['expiresAt'] == now.to_iso8601_string()

    # check that the follower_manager was called correctly
    assert post.follower_manager.mock_calls == [
        mock.call.refresh_first_story(story_prev=post_org_item, story_now=post.item),
    ]


def test_clear_expires_at(post_with_expiration):
    # add a post with an expires at
    post = post_with_expiration
    assert 'expiresAt' in post.item

    # remove the expires at
    post.follower_manager = mock.Mock(post.follower_manager)
    post_org_item = post.item.copy()
    post.set_expires_at(None)
    assert 'expiresAt' not in post.item

    # make sure that stuck in db
    post.refresh_item()
    assert 'expiresAt' not in post.item

    # check that the follower_manager was called correctly
    assert post.follower_manager.mock_calls == [
        mock.call.refresh_first_story(story_prev=post_org_item, story_now=None),
    ]


def test_set(post, user):
    username = user.item['username']
    org_text = post.item['text']

    # verify starting values
    assert post.item['text'] == org_text
    assert post.item['textTags'] == []
    assert post.item.get('commentsDisabled', False) is False
    assert post.item.get('likesDisabled', False) is False
    assert post.item.get('sharingDisabled', False) is False
    assert post.item.get('verificationHidden', False) is False

    # do some edits
    new_text = f'its a new dawn, right @{user.item["username"]}, its a new day'
    post.set(
        text=new_text,
        comments_disabled=True,
        likes_disabled=True,
        sharing_disabled=True,
        verification_hidden=True,
    )

    # verify new values
    assert post.item['text'] == new_text
    assert post.item['textTags'] == [{'tag': f'@{username}', 'userId': user.id}]
    assert post.item.get('commentsDisabled', False) is True
    assert post.item.get('likesDisabled', False) is True
    assert post.item.get('sharingDisabled', False) is True
    assert post.item.get('verificationHidden', False) is True

    # edit some params, ignore others
    post.set(likes_disabled=False, verification_hidden=False)

    # verify only edited values changed
    assert post.item['text'] == new_text
    assert post.item['textTags'] == [{'tag': f'@{username}', 'userId': user.id}]
    assert post.item.get('commentsDisabled', False) is True
    assert post.item.get('likesDisabled', False) is False
    assert post.item.get('sharingDisabled', False) is True
    assert post.item.get('verificationHidden', False) is False

    # set keywords
    keywords = ['bird', 'tea', 'mine']
    post.set(keywords=keywords)
    assert post.item['keywords'].sort() == keywords.sort()


def test_set_cant_create_contentless_post(post_manager, post):
    org_text = post.item['text']

    # verify the post is text-only
    assert org_text
    assert not post.image_item

    # verify we can't set the text to null on that post
    with pytest.raises(PostException):
        post.set(text='')

    # check no changes anywhere
    assert post.item['text'] == org_text
    post.refresh_item()
    assert post.item['text'] == org_text


def test_set_text_to_null_media_post(post_manager, post_with_media):
    post = post_with_media
    org_text = post.item['text']

    # verify the post has media and text
    assert org_text
    assert post.image_item

    # verify we can null out the text on that post if we want
    post.set(text='')
    assert 'text' not in post.item
    assert 'textTags' not in post.item
    post.refresh_item()
    assert 'text' not in post.item
    assert 'textTags' not in post.item


def test_serailize(user, post, user_manager):
    resp = post.serialize('caller-uid')
    assert resp.pop('postedBy')['userId'] == user.id
    assert resp == post.item


def test_error_failure(post_manager, post):
    # verify can't change a completed post to error
    with pytest.raises(PostException, match='PENDING'):
        post.error('not used')


def test_error_pending_post(post_manager, user):
    # create a pending post
    post = post_manager.add_post(user, 'pid2', PostType.IMAGE)
    assert post.item['postStatus'] == PostStatus.PENDING
    assert 'postStatusReason' not in post.item['postStatus']

    # error it out, verify in-mem copy got marked as such
    post.error('just because')
    assert post.item['postStatus'] == PostStatus.ERROR
    assert post.item['postStatusReason'] == 'just because'
    assert post.item == post.refresh_item().item


def test_error_processing_post(post_manager, user):
    # create a pending post
    post = post_manager.add_post(user, 'pid2', PostType.IMAGE)

    # manually mark the Post as being processed
    post.item = post.dynamo.set_post_status(post.item, PostStatus.PROCESSING)
    assert post.item['postStatus'] == PostStatus.PROCESSING
    assert 'postStatusReason' not in post.item['postStatus']

    # error it out, verify in-mem copy got marked as such
    post.error('of course')
    assert post.item['postStatus'] == PostStatus.ERROR
    assert post.item['postStatusReason'] == 'of course'
    assert post.item == post.refresh_item().item


def test_set_album_errors(album_manager, post_manager, user_manager, post, post_with_media, user):
    # album doesn't exist
    with pytest.raises(PostException, match='does not exist'):
        post_with_media.set_album('aid-dne')

    # album is owned by a different user
    album = album_manager.add_album(user.id, 'aid-2', 'album name')
    with pytest.raises(PostException, match='belong to different users'):
        post_with_media.set_album(album.id)


def test_set_album_completed_post(albums, post_with_media):
    post = post_with_media
    album1, album2 = albums

    # verify starting state
    assert 'albumId' not in post.item
    assert album1.item.get('rankCount', 0) == 0
    assert album2.item.get('rankCount', 0) == 0

    # go from no album to an album
    post.set_album(album1.id)
    assert post.item['albumId'] == album1.id
    assert post.item['gsiK3SortKey'] == 0  # album rank
    assert album1.refresh_item().item.get('rankCount', 0) == 1
    assert album2.refresh_item().item.get('rankCount', 0) == 0

    # change the album
    post.set_album(album2.id)
    assert post.item['albumId'] == album2.id
    assert post.item['gsiK3SortKey'] == 0  # album rank
    assert album1.refresh_item().item.get('rankCount', 0) == 1
    assert album2.refresh_item().item.get('rankCount', 0) == 1

    # no-op
    post.set_album(album2.id)
    assert post.item['albumId'] == album2.id
    assert post.item['gsiK3SortKey'] == 0  # album rank
    assert album1.refresh_item().item.get('rankCount', 0) == 1
    assert album2.refresh_item().item.get('rankCount', 0) == 1

    # remove post from all albums
    post.set_album(None)
    assert 'albumId' not in post.item
    assert 'gsiK3SortKey' not in post.item
    assert album1.refresh_item().item.get('rankCount', 0) == 1
    assert album2.refresh_item().item.get('rankCount', 0) == 1

    # archive the post
    post.archive()

    # add it back to an album, should not increment counts
    post.set_album(album1.id)
    assert post.item['albumId'] == album1.id
    assert post.item['gsiK3SortKey'] == -1  # album rank
    assert album1.refresh_item().item.get('rankCount', 0) == 1


def test_set_album_order_failures(user, user2, albums, post_manager, image_data_b64):
    post1 = post_manager.add_post(user, 'pid1', PostType.IMAGE, image_input={'imageData': image_data_b64})
    post2 = post_manager.add_post(user2, 'pid2', PostType.IMAGE, image_input={'imageData': image_data_b64})
    post3 = post_manager.add_post(user2, 'pid3', PostType.IMAGE, image_input={'imageData': image_data_b64})
    post4 = post_manager.add_post(user2, 'pid4', PostType.IMAGE, image_input={'imageData': image_data_b64})
    album1, album2 = albums

    # put post2 & post3 in first album
    post2.set_album(album1.id)
    assert post2.item['albumId'] == album1.id
    assert post2.item['gsiK3SortKey'] == 0

    post3.set_album(album1.id)
    assert post3.item['albumId'] == album1.id
    assert post3.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(1 / 3))

    # put post4 in second album
    post4.set_album(album2.id)
    assert post4.item['albumId'] == album2.id
    assert post4.item['gsiK3SortKey'] == 0

    # verify can't change order with post that DNE
    with pytest.raises(PostException):
        post2.set_album_order('pid-dne')

    # verify can't change order using post from diff users
    with pytest.raises(PostException):
        post1.set_album_order(post2.id)
    with pytest.raises(PostException):
        post2.set_album_order(post1.id)

    # verify can't change order with posts in diff albums
    with pytest.raises(PostException):
        post4.set_album_order(post2.id)
    with pytest.raises(PostException):
        post2.set_album_order(post4.id)

    # verify *can* change order if everything correct
    post2.set_album_order(post3.id)
    assert post2.item['albumId'] == album1.id
    assert post2.item['gsiK3SortKey'] == decimal.Decimal(0.5)

    # verify if album no longer exists in DB, can't change order
    album1.dynamo.delete_album(album1.id)
    with pytest.raises(Exception, match='Album `.*` that post `.*` was in does not exist'):
        post3.set_album_order(post2.id)
    assert 'albumId' not in post3.refresh_item().item


def test_set_album_order_lots_of_set_middle(user2, albums, post_manager, image_data_b64):
    # album with three posts in it
    album, _ = albums
    post1 = post_manager.add_post(
        user2,
        'pid1',
        PostType.IMAGE,
        image_input={'imageData': image_data_b64},
        album_id=album.id,
    )
    post2 = post_manager.add_post(
        user2,
        'pid2',
        PostType.IMAGE,
        image_input={'imageData': image_data_b64},
        album_id=album.id,
    )
    post3 = post_manager.add_post(
        user2,
        'pid3',
        PostType.IMAGE,
        image_input={'imageData': image_data_b64},
        album_id=album.id,
    )

    # check starting state
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post1.id, post2.id, post3.id]
    assert post1.item['gsiK3SortKey'] == 0
    assert post2.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(1 / 3))
    assert post3.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(1 / 2))

    # change middle post, check order
    post3.set_album_order(post1.id)
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post1.id, post3.id, post2.id]
    assert post3.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(1 / 6))

    # change middle post, check order
    post2.set_album_order(post1.id)
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post1.id, post2.id, post3.id]
    assert post2.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(1 / 12))

    # change middle post, check order
    post3.set_album_order(post1.id)
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post1.id, post3.id, post2.id]
    assert post3.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(1 / 24))

    # change middle post, check order
    post2.set_album_order(post1.id)
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post1.id, post2.id, post3.id]
    assert post2.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(1 / 48))


def test_set_album_order_lots_of_set_front(user2, albums, post_manager, image_data_b64):
    # album with two posts in it
    album, _ = albums
    post1 = post_manager.add_post(
        user2,
        'pid1',
        PostType.IMAGE,
        image_input={'imageData': image_data_b64},
        album_id=album.id,
    )
    post2 = post_manager.add_post(
        user2,
        'pid2',
        PostType.IMAGE,
        image_input={'imageData': image_data_b64},
        album_id=album.id,
    )

    # check starting state
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post1.id, post2.id]
    assert post1.item['gsiK3SortKey'] == 0
    assert post2.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(1 / 3))

    # change first post, check order
    post2.set_album_order(None)
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post2.id, post1.id]
    assert post2.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(-2 / 4))

    # change first post, check order
    post1.set_album_order(None)
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post1.id, post2.id]
    assert post1.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(-3 / 5))

    # change first post, check order
    post2.set_album_order(None)
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post2.id, post1.id]
    assert post2.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(-4 / 6))


def test_set_album_order_lots_of_set_back(user2, albums, post_manager, image_data_b64):
    # album with two posts in it
    album, _ = albums
    post1 = post_manager.add_post(
        user2,
        'pid1',
        PostType.IMAGE,
        image_input={'imageData': image_data_b64},
        album_id=album.id,
    )
    post2 = post_manager.add_post(
        user2,
        'pid2',
        PostType.IMAGE,
        image_input={'imageData': image_data_b64},
        album_id=album.id,
    )

    # check starting state
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post1.id, post2.id]
    assert post1.item['gsiK3SortKey'] == 0
    assert post2.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(1 / 3))

    # change last post, check order
    post1.set_album_order(post2.id)
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post2.id, post1.id]
    assert post1.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(2 / 4))

    # change last post, check order
    post2.set_album_order(post1.id)
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post1.id, post2.id]
    assert post2.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(3 / 5))

    # change last post, check order
    post1.set_album_order(post2.id)
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post2.id, post1.id]
    assert post1.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(4 / 6))


def test_set_album_order_no_op(user2, albums, post_manager, image_data_b64):
    # album with two posts in it
    album, _ = albums
    post1 = post_manager.add_post(
        user2,
        'pid1',
        PostType.IMAGE,
        image_input={'imageData': image_data_b64},
        album_id=album.id,
    )
    post2 = post_manager.add_post(
        user2,
        'pid2',
        PostType.IMAGE,
        image_input={'imageData': image_data_b64},
        album_id=album.id,
    )

    # check starting state
    assert album.refresh_item().item
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post1.id, post2.id]
    assert post1.item['gsiK3SortKey'] == 0
    assert post2.item['gsiK3SortKey'] == pytest.approx(decimal.Decimal(1 / 3))

    # set post1 to first position, which it already is in
    post1.set_album_order(None)
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post1.id, post2.id]
    assert post1.item == post1.refresh_item().item
    assert album.item == album.refresh_item().item

    # set post2 to the 2nd position, which it already is in
    post2.set_album_order(post1.id)
    assert list(post_manager.dynamo.generate_post_ids_in_album(album.id)) == [post1.id, post2.id]
    assert post2.item == post2.refresh_item().item
    assert album.item == album.refresh_item().item


def test_build_image_thumbnails_video_post(user, processing_video_post, s3_uploads_client):
    post = processing_video_post

    # check starting state
    assert s3_uploads_client.exists(post.get_image_path(image_size.NATIVE))
    assert not s3_uploads_client.exists(post.get_image_path(image_size.K4))
    assert not s3_uploads_client.exists(post.get_image_path(image_size.P1080))
    assert not s3_uploads_client.exists(post.get_image_path(image_size.P480))
    assert not s3_uploads_client.exists(post.get_image_path(image_size.P64))

    # build the thumbnails
    post.build_image_thumbnails()

    # check final state
    assert s3_uploads_client.exists(post.get_image_path(image_size.NATIVE))
    assert s3_uploads_client.exists(post.get_image_path(image_size.K4))
    assert s3_uploads_client.exists(post.get_image_path(image_size.P1080))
    assert s3_uploads_client.exists(post.get_image_path(image_size.P480))
    assert s3_uploads_client.exists(post.get_image_path(image_size.P64))


def test_get_image_writeonly_url(pending_image_post, cloudfront_client, dynamo_client):
    post = pending_image_post
    post.cloudfront_client = cloudfront_client

    # check a jpg image post
    assert post.get_image_writeonly_url()
    assert 'native.jpg' in cloudfront_client.generate_presigned_url.call_args.args[0]
    assert 'native.heic' not in cloudfront_client.generate_presigned_url.call_args.args[0]

    # set the imageFormat to heic
    dynamo_client.set_attributes({'partitionKey': f'post/{post.id}', 'sortKey': 'image'}, imageFormat='HEIC')
    post.refresh_image_item()

    # check a heic image post
    assert post.get_image_writeonly_url()
    assert 'native.jpg' not in cloudfront_client.generate_presigned_url.call_args.args[0]
    assert 'native.heic' in cloudfront_client.generate_presigned_url.call_args.args[0]


def test_set_height_and_width(s3_uploads_client, pending_image_post):
    post = pending_image_post
    assert 'height' not in post.image_item
    assert 'width' not in post.image_item

    # put an image in the bucket
    s3_path = post.get_image_path(image_size.NATIVE)
    s3_uploads_client.put_object(s3_path, open(grant_path, 'rb'), 'image/jpeg')

    post.set_height_and_width()
    assert post.image_item['height'] == grant_height
    assert post.image_item['width'] == grant_width
    post.refresh_image_item()
    assert post.image_item['height'] == grant_height
    assert post.image_item['width'] == grant_width


def test_set_colors(s3_uploads_client, pending_image_post):
    post = pending_image_post
    assert 'colors' not in post.image_item

    # put an image in the bucket
    s3_path = post.get_image_path(image_size.NATIVE)
    s3_uploads_client.put_object(s3_path, open(grant_path, 'rb'), 'image/jpeg')

    post.set_colors()
    assert post.image_item['colors'] == grant_colors


def test_set_colors_colortheif_fails(s3_uploads_client, pending_image_post, caplog):
    post = pending_image_post
    assert 'colors' not in post.image_item

    # put an image in the bucket
    s3_path = post.get_image_path(image_size.NATIVE)
    s3_uploads_client.put_object(s3_path, open(blank_path, 'rb'), 'image/jpeg')

    assert len(caplog.records) == 0
    with caplog.at_level(logging.WARNING):
        post.set_colors()
        assert 'colors' not in post.image_item

    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert 'ColorTheif' in caplog.records[0].msg
    assert f'`{post.id}`' in caplog.records[0].msg


def test_trending_increment_score_success_case(image_post):
    org_score = image_post.trending_item['gsiA4SortKey']
    recorded = image_post.trending_increment_score()
    assert recorded is True
    assert image_post.trending_item['gsiA4SortKey'] > org_score


def test_trending_increment_score_skips_non_original_posts(image_post):
    org_score = image_post.trending_item['gsiA4SortKey']
    image_post.item['originalPostId'] = str(uuid.uuid4())
    recorded = image_post.trending_increment_score()
    assert recorded is False
    assert image_post.trending_item['gsiA4SortKey'] == org_score


def test_trending_increment_score_skip_real_users_posts(real_user, post_manager, grant_data_b64):
    image_post = post_manager.add_post(
        real_user, str(uuid.uuid4()), PostType.IMAGE, image_input={'imageData': grant_data_b64}
    )
    assert image_post.trending_item is None
    recorded = image_post.trending_increment_score()
    assert recorded is False
    assert image_post.trending_item is None


def test_trending_increment_score_skip_posts_over_24_hours_old(post):
    # verify we can increment its trending as normal
    org_score = post.trending_item['gsiA4SortKey']
    post.item['originalPostId'] = str(uuid.uuid4())
    recorded = post.trending_increment_score()
    assert recorded is True
    assert post.trending_item['gsiA4SortKey'] != org_score

    # hack it's age to 23:59, verify we can increment its trending
    post.item['postedAt'] = pendulum.now('utc').subtract(hours=23, minutes=59).to_iso8601_string()
    org_score = post.trending_item['gsiA4SortKey']
    post.item['originalPostId'] = str(uuid.uuid4())
    recorded = post.trending_increment_score()
    assert recorded is True
    assert post.trending_item['gsiA4SortKey'] != org_score

    # hack it's age to 24hrs + a few microseconds, verify we cannot increment its trending anymore
    post.item['postedAt'] = pendulum.now('utc').subtract(hours=24).to_iso8601_string()
    org_score = post.trending_item['gsiA4SortKey']
    post.item['originalPostId'] = str(uuid.uuid4())
    recorded = post.trending_increment_score()
    assert recorded is False
    assert post.trending_item['gsiA4SortKey'] == org_score


def test_get_trending_multiplier(post):
    # walk through the matrix, test them all
    assert 'isVerifed' not in post.item
    assert 'subscriptionLevel' not in post.user.item
    assert post.get_trending_multiplier() == 1

    post.user.item['subscriptionLevel'] = UserSubscriptionLevel.DIAMOND
    assert post.get_trending_multiplier() == 4

    post.user.item['subscriptionLevel'] = UserSubscriptionLevel.BASIC
    assert post.get_trending_multiplier() == 1

    post.item['isVerified'] = None
    assert post.get_trending_multiplier() == 1

    post.item['isVerified'] = True
    assert post.get_trending_multiplier() == 1

    post.item['isVerified'] = False
    assert post.get_trending_multiplier() == 0.5

    post.user.item['subscriptionLevel'] = UserSubscriptionLevel.DIAMOND
    assert post.get_trending_multiplier() == 2

    # THUMBNAIL views get the same as the default, FOCUS views get a 2x boost
    default = post.get_trending_multiplier()
    assert post.get_trending_multiplier(view_type=ViewType.THUMBNAIL) == default
    assert post.get_trending_multiplier(view_type=ViewType.FOCUS) == default * 2
