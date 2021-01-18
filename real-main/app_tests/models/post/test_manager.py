import logging
import uuid
from unittest.mock import call, patch

import pendulum
import pytest
import stringcase

from app.mixins.view.enums import ViewType
from app.models.post.enums import PostStatus, PostType
from app.models.post.exceptions import PostException
from app.utils import image_size


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user


@pytest.fixture
def posts(post_manager, user):
    post1 = post_manager.add_post(user, 'pid1', PostType.TEXT_ONLY, text='t')
    post2 = post_manager.add_post(user, 'pid2', PostType.TEXT_ONLY, text='t')
    yield (post1, post2)


@pytest.fixture
def album(album_manager, user):
    yield album_manager.add_album(user.id, 'aid', 'album name')


def test_get_post(post_manager, user):
    # create a post behind the scenes
    post_id = 'pid'
    post_manager.add_post(user, post_id, PostType.TEXT_ONLY, text='t')

    post = post_manager.get_post(post_id)
    assert post.id == post_id


def test_get_post_dne(post_manager):
    assert post_manager.get_post('pid-dne') is None


def test_add_post_errors(post_manager, user):
    # try to add a post without any content (no text or media)
    with pytest.raises(PostException, match='without text'):
        post_manager.add_post(user, 'pid', PostType.TEXT_ONLY)

    # try to add a post with a negative lifetime value
    lifetime_duration = pendulum.duration(hours=-1)
    with pytest.raises(PostException, match='non-positive lifetime'):
        post_manager.add_post(user, 'pid', PostType.TEXT_ONLY, text='t', lifetime_duration=lifetime_duration)

    # try to add a post with a zero lifetime value
    lifetime_duration = pendulum.duration(hours=0)
    with pytest.raises(PostException, match='non-positive lifetime'):
        post_manager.add_post(user, 'pid', PostType.TEXT_ONLY, text='t', lifetime_duration=lifetime_duration)

    # try to add a text-only post with a media_upload
    with pytest.raises(PostException, match='with ImageInput'):
        post_manager.add_post(user, 'pid', PostType.TEXT_ONLY, text='t', image_input={'mediaId': 'mid'})

    # try to add a text-only post with a media_upload
    with pytest.raises(PostException, match='with setAsUserPhoto'):
        post_manager.add_post(user, 'pid', PostType.TEXT_ONLY, text='t', set_as_user_photo=True)

    # try to add a text-only post with no text
    with pytest.raises(PostException, match='without text'):
        post_manager.add_post(user, 'pid', PostType.TEXT_ONLY)

    # try to add a video post with a media_upload
    with pytest.raises(PostException, match='with ImageInput'):
        post_manager.add_post(user, 'pid', PostType.VIDEO, image_input={'mediaId': 'mid'})

    # try to add a video post as profile pic
    with pytest.raises(PostException, match='with setAsUserPhoto'):
        post_manager.add_post(user, 'pid', PostType.VIDEO, set_as_user_photo=True)

    # try to add post with invalid post type
    with pytest.raises(Exception, match='Invalid PostType'):
        post_manager.add_post(user, 'pid', 'notaposttype')


@pytest.mark.parametrize(
    'crop',
    [
        {'upperLeft': {'x': -1, 'y': 0}, 'lowerRight': {'x': 100, 'y': 100}},
        {'upperLeft': {'x': 0, 'y': -1}, 'lowerRight': {'x': 100, 'y': 100}},
        {'upperLeft': {'x': 0, 'y': 0}, 'lowerRight': {'x': -1, 'y': 100}},
        {'upperLeft': {'x': 0, 'y': 0}, 'lowerRight': {'x': 100, 'y': -1}},
    ],
)
def test_add_post_negative_crop_cordinate_errors(post_manager, crop, user):
    with pytest.raises(PostException, match='cannot be negative'):
        post_manager.add_post(user.id, 'pid', PostType.IMAGE, image_input={'crop': crop})


@pytest.mark.parametrize(
    'crop',
    [
        {'upperLeft': {'x': 0, 'y': 50}, 'lowerRight': {'x': 100, 'y': 50}},
        {'upperLeft': {'x': 100, 'y': 0}, 'lowerRight': {'x': 10, 'y': 100}},
    ],
)
def test_add_post_emptry_crop_area_errors(post_manager, crop, user):
    with pytest.raises(PostException, match='must be strictly greater than'):
        post_manager.add_post(user.id, 'pid', PostType.IMAGE, image_input={'crop': crop})


def test_add_text_only_post(post_manager, user):
    post_id = 'pid'
    text = 'lore ipsum'
    now = pendulum.now('utc')

    # add the post
    post_manager.add_post(user, post_id, PostType.TEXT_ONLY, text=text, now=now)

    # retrieve the post & media, check it
    post = post_manager.get_post(post_id)
    assert post.id == post_id
    assert post.item['postedByUserId'] == user.id
    assert post.item['postedAt'] == now.to_iso8601_string()
    assert post.item['text'] == 'lore ipsum'
    assert post.item['textTags'] == []
    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert 'expiresAt' not in post.item
    assert not post.image_item


def test_add_text_with_tags_post(post_manager, user):
    post_id = 'pid'
    text = f'Tagging you @{user.username}!'

    # add the post
    post_manager.add_post(user, post_id, PostType.TEXT_ONLY, text=text)

    # retrieve the post & media, check it
    post = post_manager.get_post(post_id)
    assert post.id == post_id
    assert post.item['text'] == text
    assert post.item['textTags'] == [{'tag': f'@{user.username}', 'userId': user.id}]


def test_add_post_album_errors(user_manager, post_manager, user, album, user2):
    # can't create post with album that doesn't exist
    with pytest.raises(PostException, match='does not exist'):
        post_manager.add_post(user, 'pid-4', PostType.IMAGE, album_id='aid-dne')

    # can't create post in somebody else's album
    with pytest.raises(PostException, match='does not belong to'):
        post_manager.add_post(user2, 'pid-4', PostType.IMAGE, album_id=album.id)

    # verify we can add without error
    post_manager.add_post(user, 'pid-42', PostType.IMAGE, album_id=album.id)


def test_add_text_only_post_to_album(post_manager, user, album):
    post_id = 'pid'

    # add the post, check all looks good
    post = post_manager.add_post(user, post_id, PostType.TEXT_ONLY, text='t', album_id=album.id)
    assert post.id == post_id
    assert post.item['albumId'] == album.id
    assert post.item['gsiK3SortKey'] == 0  # album rank

    post.refresh_item()
    assert post.item['albumId'] == album.id
    assert post.item['gsiK3SortKey'] == 0  # album rank
    assert album.refresh_item().item['rankCount'] == 1


def test_video_post_to_album(post_manager, user, album, s3_uploads_client, grant_data):
    post_id = 'pid'

    # add the post, check all looks good
    post = post_manager.add_post(user, post_id, PostType.VIDEO, album_id=album.id)
    assert post.id == post_id
    assert post.item['albumId'] == album.id
    assert post.item['gsiK3SortKey'] == -1  # album rank
    assert 'rankCount' not in album.refresh_item().item

    # complete the video post
    post.item = post.dynamo.set_post_status(post.item, PostStatus.PROCESSING)
    image_path = post.get_image_path(image_size.NATIVE)
    s3_uploads_client.put_object(image_path, grant_data, 'image/jpeg')
    post.build_image_thumbnails()
    post.complete()
    assert post.item['albumId'] == album.id
    assert post.item['gsiK3SortKey'] == 0  # album rank

    post.refresh_item()
    assert post.item['albumId'] == album.id
    assert post.item['gsiK3SortKey'] == 0  # album rank
    assert album.refresh_item().item['rankCount'] == 1


def test_add_video_post_minimal(post_manager, user):
    post_id = 'pid'

    # add the post
    post_manager.add_post(user, post_id, PostType.VIDEO)

    # retrieve the post & media, check it
    post = post_manager.get_post(post_id)
    assert post.id == post_id
    assert post.item['postType'] == PostType.VIDEO
    assert post.item['postedByUserId'] == user.id
    assert post.item['postedAt']
    assert post.item['postStatus'] == PostStatus.PENDING
    assert 'text' not in post.item
    assert 'textTags' not in post.item
    assert 'expiresAt' not in post.item
    assert not post.image_item


def test_add_video_post_maximal(post_manager, user):
    post_id = 'pid'
    text = f'from lore to ipsum, right @{user.username}?'
    now = pendulum.now('utc')
    lifetime_duration = pendulum.duration(hours=1)
    comments_disabled = True
    likes_disabled = True
    sharing_disabled = True
    verification_hidden = True
    expires_at = now + lifetime_duration

    # add the post
    post_manager.add_post(
        user,
        post_id,
        PostType.VIDEO,
        text=text,
        lifetime_duration=lifetime_duration,
        comments_disabled=comments_disabled,
        likes_disabled=likes_disabled,
        sharing_disabled=sharing_disabled,
        verification_hidden=verification_hidden,
        now=now,
    )

    # retrieve the post & media, check it
    post = post_manager.get_post(post_id)
    assert post.id == post_id
    assert post.item['postType'] == PostType.VIDEO
    assert post.item['postedByUserId'] == user.id
    assert post.item['postedAt'] == now.to_iso8601_string()
    assert post.item['postStatus'] == PostStatus.PENDING
    assert post.item['text'] == text
    assert len(post.item['textTags']) == 1
    assert post.item['expiresAt'] == expires_at.to_iso8601_string()
    assert not post.image_item
    assert post.item['commentsDisabled'] is True
    assert post.item['likesDisabled'] is True
    assert post.item['sharingDisabled'] is True
    assert post.item['verificationHidden'] is True


def test_add_image_post_no_image_input(post_manager, user):
    post_id = 'pid'
    now = pendulum.now('utc')

    # add the post (& media)
    post_manager.add_post(user, post_id, PostType.IMAGE, now=now)

    # retrieve the post & media, check it
    post = post_manager.get_post(post_id)
    assert post.id == post_id
    assert post.item['postedByUserId'] == user.id
    assert post.item['postedAt'] == now.to_iso8601_string()
    assert post.item['postStatus'] == PostStatus.PENDING
    assert 'text' not in post.item
    assert 'textTags' not in post.item
    assert 'expiresAt' not in post.item
    assert not post.image_item


def test_add_image_post_text_empty_string(post_manager, user):
    post_id = 'pid'
    now = pendulum.now('utc')

    # add the post (& media)
    post_manager.add_post(user, post_id, PostType.IMAGE, now=now, text='')

    # retrieve the post & media, check it
    post = post_manager.get_post(post_id)
    assert post.id == post_id
    assert 'text' not in post.item
    assert 'textTags' not in post.item
    assert not post.image_item


def test_add_image_post_with_image_data(user, post_manager, grant_data_b64):
    post_id = 'pid'
    now = pendulum.now('utc')
    image_input = {'imageData': grant_data_b64}

    # add the post (& media)
    post_manager.add_post(user, post_id, PostType.IMAGE, now=now, image_input=image_input)

    # retrieve the post & media, check it
    post = post_manager.get_post(post_id)
    assert post.id == post_id
    assert post.item['postedByUserId'] == user.id
    assert post.item['postedAt'] == now.to_iso8601_string()
    assert post.item['postStatus'] == PostStatus.COMPLETED
    assert 'text' not in post.item
    assert 'textTags' not in post.item
    assert 'expiresAt' not in post.item
    assert post.image_item['partitionKey'] == post.item['partitionKey']
    assert post.image_item['sortKey'] == 'image'


def test_add_image_post_with_image_data_processing_error(user, post_manager, grant_data_b64):
    post_id = 'pid'
    now = pendulum.now('utc')
    image_input = {'imageData': grant_data_b64[:12]}  # truncated jpeg data is invalid but correctly b64-encoded

    # add the post (& media)
    post_manager.add_post(user, post_id, PostType.IMAGE, now=now, image_input=image_input)

    # retrieve the post & media, check it
    post = post_manager.get_post(post_id)
    assert post.id == post_id
    assert post.item['postedByUserId'] == user.id
    assert post.item['postedAt'] == now.to_iso8601_string()
    assert post.item['postStatus'] == PostStatus.ERROR
    assert post.item['postStatusReason'].startswith('Unable to recognize file type of uploaded file for post ')
    assert post.id in post.item['postStatusReason']


def test_add_image_post_with_options(post_manager, album, user):
    post_id = 'pid'
    text = 'lore ipsum'
    now = pendulum.now('utc')
    crop = {'upperLeft': {'x': 1, 'y': 2}, 'lowerRight': {'x': 4, 'y': 3}}
    image_input = {
        'crop': crop,
        'takenInReal': False,
        'originalFormat': 'org-format',
        'originalMetadata': 'org-metadata',
    }
    lifetime_duration = pendulum.duration(hours=1)

    # add the post (& media)
    post_manager.add_post(
        user,
        post_id,
        PostType.IMAGE,
        text=text,
        now=now,
        image_input=image_input,
        lifetime_duration=lifetime_duration,
        album_id=album.id,
        comments_disabled=False,
        likes_disabled=True,
        verification_hidden=False,
        set_as_user_photo=True,
    )
    expires_at = now + lifetime_duration

    # retrieve the post & media, check it
    post = post_manager.get_post(post_id)
    assert post.id == post_id
    assert post.item['postedByUserId'] == user.id
    assert post.item['albumId'] == album.id
    assert post.item['postedAt'] == now.to_iso8601_string()
    assert post.item['text'] == 'lore ipsum'
    assert post.item['postStatus'] == PostStatus.PENDING
    assert post.item['expiresAt'] == expires_at.to_iso8601_string()
    assert post.item['commentsDisabled'] is False
    assert post.item['likesDisabled'] is True
    assert post.item['verificationHidden'] is False
    assert post.item['setAsUserPhoto'] is True

    post_original_metadata = post_manager.original_metadata_dynamo.get(post_id)
    assert post_original_metadata['originalMetadata'] == 'org-metadata'

    assert post.image_item['crop'] == crop
    assert post.image_item['takenInReal'] is False
    assert post.image_item['originalFormat'] == 'org-format'


def test_add_post_settings_default_to_user_level_settings(post_manager, user):
    # set user-level defaults
    defaults = {
        'comments_disabled': False,
        'likes_disabled': True,
        'sharing_disabled': False,
        'verification_hidden': True,
    }
    user.update_details(**defaults)

    # create a post with all the settings speficied to the same as user defaults, verify all good
    post = post_manager.add_post(user, str(uuid.uuid4()), PostType.IMAGE, **defaults)
    for k, v in defaults.items():
        assert post.item.get(stringcase.camelcase(k)) is v

    # create a post with all the settings speficied to the opposite as user defaults, verify all good
    post = post_manager.add_post(
        user, str(uuid.uuid4()), PostType.IMAGE, **{k: not v for k, v in defaults.items()}
    )
    for k, v in defaults.items():
        assert post.item.get(stringcase.camelcase(k)) is (not v)

    # create a post with no settings speficied, verify user defaults taken
    post = post_manager.add_post(user, str(uuid.uuid4()), PostType.IMAGE)
    for k, v in defaults.items():
        assert post.item.get(stringcase.camelcase(k)) is v


def test_delete_recently_expired_posts(post_manager, user, caplog):
    now = pendulum.now('utc')

    # create four posts with diff. expiration qualities
    post_no_expires = post_manager.add_post(user, 'pid1', PostType.TEXT_ONLY, text='t')
    assert 'expiresAt' not in post_no_expires.item

    post_future_expires = post_manager.add_post(
        user, 'pid2', PostType.TEXT_ONLY, text='t', lifetime_duration=pendulum.duration(hours=1)
    )
    assert post_future_expires.item['expiresAt'] > now.to_iso8601_string()

    lifetime_duration = pendulum.duration(hours=now.hour, minutes=now.minute)
    post_expired_today = post_manager.add_post(
        user,
        'pid3',
        PostType.TEXT_ONLY,
        text='t',
        lifetime_duration=lifetime_duration,
        now=(now - lifetime_duration),
    )
    assert post_expired_today.item['expiresAt'] == now.to_iso8601_string()

    post_expired_last_week = post_manager.add_post(
        user,
        'pid4',
        PostType.TEXT_ONLY,
        text='t',
        lifetime_duration=pendulum.duration(hours=1),
        now=(now - pendulum.duration(days=7)),
    )
    assert post_expired_last_week.item['expiresAt'] < (now - pendulum.duration(days=6)).to_iso8601_string()

    # run the deletion run
    post_manager.delete_recently_expired_posts()

    # check we logged one delete
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert 'Deleting' in caplog.records[0].msg
    assert post_expired_today.id in caplog.records[0].msg

    # check one of the posts is missing from the DB, but the rest are still there
    assert post_no_expires.refresh_item().item
    assert post_future_expires.refresh_item().item
    assert post_expired_today.refresh_item().item is None
    assert post_expired_last_week.refresh_item().item


def test_delete_older_expired_posts(post_manager, user, caplog):
    now = pendulum.now('utc')

    # create four posts with diff. expiration qualities
    post_no_expires = post_manager.add_post(user, 'pid1', PostType.TEXT_ONLY, text='t')
    assert 'expiresAt' not in post_no_expires.item

    post_future_expires = post_manager.add_post(
        user,
        'pid2',
        PostType.TEXT_ONLY,
        text='t',
        lifetime_duration=pendulum.duration(hours=1),
    )
    assert post_future_expires.item['expiresAt'] > now.to_iso8601_string()

    lifetime_duration = pendulum.duration(hours=now.hour, minutes=now.minute)
    post_expired_today = post_manager.add_post(
        user,
        'pid3',
        PostType.TEXT_ONLY,
        text='t',
        lifetime_duration=lifetime_duration,
        now=(now - lifetime_duration),
    )
    assert post_expired_today.item['expiresAt'] == now.to_iso8601_string()

    post_expired_last_week = post_manager.add_post(
        user,
        'pid4',
        PostType.TEXT_ONLY,
        text='t',
        lifetime_duration=pendulum.duration(hours=1),
        now=(now - pendulum.duration(days=7)),
    )
    assert post_expired_last_week.item['expiresAt'] < (now - pendulum.duration(days=6)).to_iso8601_string()

    # run the deletion run
    post_manager.delete_older_expired_posts()

    # check we logged one delete
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert 'Deleting' in caplog.records[0].msg
    assert post_expired_last_week.id in caplog.records[0].msg

    # check one of the posts is missing from the DB, but the rest are still there
    assert post_no_expires.refresh_item().item
    assert post_future_expires.refresh_item().item
    assert post_expired_today.refresh_item().item
    assert post_expired_last_week.refresh_item().item is None


def test_set_post_status_to_error(post_manager, user_manager, user):
    # create a COMPLETED post, verify cannot transition it to ERROR
    post = post_manager.add_post(user, 'pid1', PostType.TEXT_ONLY, text='t')
    with pytest.raises(PostException, match='PENDING'):
        post.error('this reason')

    # add a PENDING post, transition it to ERROR, verify all good
    post = post_manager.add_post(user, 'pid', PostType.IMAGE)
    reason = 'just because'
    post.error(reason)
    assert post.item['postStatus'] == PostStatus.ERROR
    assert post.item['postStatusReason'] == reason
    assert post.item == post.refresh_item().item


def test_record_views(post_manager, user, user2, posts, caplog):
    post1, post2 = posts

    # cant record view to post that dne
    with caplog.at_level(logging.WARNING):
        post_manager.record_views(['pid-dne'], user2.id)
    assert len(caplog.records) == 1
    assert 'on DNE post' in caplog.records[0].msg
    assert 'pid-dne' in caplog.records[0].msg
    assert user2.id in caplog.records[0].msg
    assert 'lastPostViewAt' not in user2.refresh_item().item

    # recording views on our own post
    assert post_manager.view_dynamo.get_view(post1.id, user.id) is None
    assert post_manager.view_dynamo.get_view(post2.id, user.id) is None
    assert 'postLastViewAt' not in user.refresh_item().item
    post_manager.record_views([post1.id, post2.id], user.id)
    assert post_manager.view_dynamo.get_view(post1.id, user.id)['viewCount'] == 1
    assert post_manager.view_dynamo.get_view(post2.id, user.id)['viewCount'] == 1
    assert user.refresh_item().item['lastPostViewAt']

    # another user can record views of our posts
    assert post_manager.view_dynamo.get_view(post1.id, user2.id) is None
    assert post_manager.view_dynamo.get_view(post2.id, user2.id) is None
    assert 'postLastViewAt' not in user2.refresh_item().item
    post_manager.record_views([post1.id, post2.id, post1.id], user2.id)
    assert post_manager.view_dynamo.get_view(post1.id, user2.id)['viewCount'] == 2
    assert post_manager.view_dynamo.get_view(post2.id, user2.id)['viewCount'] == 1
    assert user2.refresh_item().item['lastPostViewAt']

    # record views of our post with focus view type
    post_manager.record_views([post1.id, post2.id, post1.id], user2.id, None, ViewType.FOCUS)
    assert post_manager.view_dynamo.get_view(post1.id, user2.id)['viewCount'] == 4
    assert post_manager.view_dynamo.get_view(post2.id, user2.id)['viewCount'] == 2
    assert post_manager.view_dynamo.get_view(post1.id, user2.id)['focusViewCount'] == 2
    assert post_manager.view_dynamo.get_view(post2.id, user2.id)['focusViewCount'] == 1
    assert user2.refresh_item().item['lastPostViewAt']
    assert user2.refresh_item().item['lastPostFocusViewAt']

    # record views of our post with thumbnail view type
    post_manager.record_views([post1.id, post2.id], user.id, None, ViewType.THUMBNAIL)
    assert post_manager.view_dynamo.get_view(post1.id, user.id)['viewCount'] == 2
    assert post_manager.view_dynamo.get_view(post2.id, user.id)['viewCount'] == 2
    assert post_manager.view_dynamo.get_view(post1.id, user.id)['thumbnailViewCount'] == 1
    assert post_manager.view_dynamo.get_view(post2.id, user.id)['thumbnailViewCount'] == 1
    assert user2.refresh_item().item['lastPostViewAt']
    assert user2.refresh_item().item['lastPostFocusViewAt']


def test_add_post_with_keywords_attribute(post_manager, user):
    # create a post behind the scenes
    post_id = 'pid'
    keywords = ['bird', 'mine', 'tea']
    post_manager.add_post(user, post_id, PostType.TEXT_ONLY, text='t', keywords=keywords)

    post = post_manager.get_post(post_id)
    assert post.id == post_id
    assert post.item['keywords'].sort() == keywords.sort()


def test_find_posts(post_manager, user):
    keywords = 'bird'
    limit = 20
    next_token = 0
    query = {
        'from': next_token,
        'size': limit,
        'query': {
            'bool': {
                'should': [
                    {'match_bool_prefix': {'keywords': {'query': keywords, 'boost': 2}}},
                    {'match': {'keywords': {'query': keywords, 'boost': 2}}},
                ],
            }
        },
    }

    with patch.object(post_manager, 'elasticsearch_client') as elasticsearch_client_mock:
        post_manager.find_posts(keywords, limit, next_token)

    assert elasticsearch_client_mock.mock_calls == [
        call.query_posts(query),
        call.query_posts().__getitem__('hits'),
        call.query_posts().__getitem__().__getitem__('hits'),
        call.query_posts().__getitem__().__getitem__().__iter__(),
    ]
