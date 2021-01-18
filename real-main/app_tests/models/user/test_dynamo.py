import logging
from uuid import uuid4

import pendulum
import pytest

from app.mixins.view.enums import ViewType
from app.models.user.dynamo import UserDynamo
from app.models.user.enums import UserDatingStatus, UserPrivacyStatus, UserStatus, UserSubscriptionLevel
from app.models.user.exceptions import UserAlreadyExists, UserAlreadyGrantedSubscription


@pytest.fixture
def user_dynamo(dynamo_client):
    yield UserDynamo(dynamo_client)


def test_add_user_minimal(user_dynamo):
    user_id = 'my-user-id'
    username = 'my-USername'

    before = pendulum.now('utc')
    item = user_dynamo.add_user(user_id, username)
    after = pendulum.now('utc')

    now = pendulum.parse(item['signedUpAt'])
    assert before < now
    assert after > now

    assert item == {
        'schemaVersion': 11,
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'gsiA1PartitionKey': f'username/{username}',
        'gsiA1SortKey': '-',
        'userId': user_id,
        'username': username,
        'privacyStatus': UserPrivacyStatus.PUBLIC,
        'signedUpAt': now.to_iso8601_string(),
    }


def test_add_user_maximal(user_dynamo):
    user_id = 'my-user-id'
    username = 'my-USername'
    full_name = 'my-full-name'
    email = 'my-email'
    phone = 'my-phone'
    photo_code = 'red-cat'
    status = UserStatus.ANONYMOUS

    now = pendulum.now('utc')
    item = user_dynamo.add_user(
        user_id,
        username,
        full_name=full_name,
        email=email,
        phone=phone,
        placeholder_photo_code=photo_code,
        status=status,
        now=now,
    )

    assert item == {
        'schemaVersion': 11,
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'gsiA1PartitionKey': f'username/{username}',
        'gsiA1SortKey': '-',
        'userId': user_id,
        'username': username,
        'userStatus': UserStatus.ANONYMOUS,
        'privacyStatus': UserPrivacyStatus.PUBLIC,
        'signedUpAt': now.to_iso8601_string(),
        'fullName': full_name,
        'email': email,
        'phoneNumber': phone,
        'placeholderPhotoCode': photo_code,
    }


def test_add_user_bad_status(user_dynamo):
    with pytest.raises(AssertionError):
        user_dynamo.add_user('user-id', 'myUsername', status='not-a-status')


def test_add_user_already_exists(user_dynamo):
    user_id = 'my-user-id'

    # add the user
    user_dynamo.add_user(user_id, 'bestusername')
    assert user_dynamo.get_user(user_id)['userId'] == user_id

    # verify we can't add them again
    with pytest.raises(UserAlreadyExists):
        user_dynamo.add_user(user_id, 'diffusername')


def test_add_user_at_specific_time(user_dynamo):
    now = pendulum.now('utc')
    user_id = 'my-user-id'
    username = 'my-USername'

    item = user_dynamo.add_user(user_id, username, now=now)
    assert item == {
        'schemaVersion': 11,
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'gsiA1PartitionKey': f'username/{username}',
        'gsiA1SortKey': '-',
        'userId': user_id,
        'username': username,
        'privacyStatus': UserPrivacyStatus.PUBLIC,
        'signedUpAt': now.to_iso8601_string(),
    }


def test_get_user_by_username(user_dynamo):
    user_id = 'my-user-id'
    username = 'my-USername'
    user_id2 = 'my-user-id2'
    username2 = 'my-USername2'

    # with nothing in the DB
    assert user_dynamo.get_user_by_username(username) is None

    # add a user, test we can get it and we can miss it
    user_dynamo.add_user(user_id, username)
    assert user_dynamo.get_user_by_username(username2) is None
    assert user_dynamo.get_user_by_username(username)['userId'] == user_id
    assert user_dynamo.get_user_by_username(username)['username'] == username

    # add another user, check we can get them both
    user_dynamo.add_user(user_id2, username2)
    assert user_dynamo.get_user_by_username(username)['userId'] == user_id
    assert user_dynamo.get_user_by_username(username2)['userId'] == user_id2


def test_delete_user(user_dynamo):
    user_id = 'my-user-id'
    username = 'my-USername'

    # add the user to the DB
    item = user_dynamo.add_user(user_id, username)
    assert item['userId'] == user_id

    # do the delete
    resp = user_dynamo.delete_user(user_id)
    assert resp == item

    # check that it was really removed from the db
    resp = user_dynamo.client.get_item(item)
    assert resp is None


def test_update_user_username(user_dynamo):
    user_id = 'my-user-id'
    old_username = 'my-USername'
    new_username = 'better-USername'

    # add user to DB
    old_item = user_dynamo.add_user(user_id, old_username)
    assert old_item['username'] == old_username

    # change their username
    now = pendulum.now('utc')
    new_item = user_dynamo.update_user_username(user_id, new_username, old_username, now=now)
    assert new_item['username'] == new_username
    assert new_item['usernameLastValue'] == old_username
    assert pendulum.parse(new_item['usernameLastChangedAt']) == now
    assert new_item['gsiA1PartitionKey'] == f'username/{new_username}'
    assert new_item['gsiA1SortKey'] == '-'

    new_item['username'] = old_item['username']
    new_item['gsiA1PartitionKey'] = old_item['gsiA1PartitionKey']
    del new_item['usernameLastValue']
    del new_item['usernameLastChangedAt']
    assert new_item == old_item


def test_set_user_photo_post_id(user_dynamo):
    user_id = 'my-user-id'
    username = 'name'
    post_id = 'mid'

    # add user to DB
    item = user_dynamo.add_user(user_id, username)
    assert item['username'] == username

    # check it starts empty
    item = user_dynamo.get_user(user_id)
    assert 'photoPostId' not in item

    # set it
    item = user_dynamo.set_user_photo_post_id(user_id, post_id)
    assert item['photoPostId'] == post_id

    # check that it really made it to the db
    item = user_dynamo.get_user(user_id)
    assert item['photoPostId'] == post_id


def test_set_user_photo_path_delete_it(user_dynamo):
    user_id = 'my-user-id'
    username = 'name'
    old_post_id = 'mid'

    # add user to DB
    item = user_dynamo.add_user(user_id, username)
    assert item['username'] == username

    # set old post id
    item = user_dynamo.set_user_photo_post_id(user_id, old_post_id)
    assert item['photoPostId'] == old_post_id

    # set new photo path, deleting it
    item = user_dynamo.set_user_photo_post_id(user_id, None)
    assert 'photoPostId' not in item

    # check that it really made it to the db
    item = user_dynamo.get_user(user_id)
    assert 'photoPostId' not in item


def test_set_user_details_doesnt_exist(user_dynamo):
    with pytest.raises(user_dynamo.client.exceptions.ConditionalCheckFailedException):
        user_dynamo.set_user_details('user-id', full_name='my-full-name')


def test_set_user_details(user_dynamo):
    user_id = 'my-user-id'
    username = 'my-username'
    date_of_birth = '1900-01-01'
    gender = 'MALE'
    location = {'latitude': 50.1, 'longitude': 50.1, 'accuracy': 50}
    height = 90
    match_age_range = {'min': 20, 'max': 50}
    match_genders = ['MALE', 'FEMALE']
    match_location_radius = 15
    match_height_range = {'min': 50, 'max': 110}

    user_dynamo.add_user('other-id-1', 'noise-1', 'cog-noise-1')
    expected_base_item = user_dynamo.add_user(user_id, username)
    assert expected_base_item['userId'] == user_id
    user_dynamo.add_user('other-id-2', 'noise-2', 'cog-noise-2')

    resp = user_dynamo.set_user_details(user_id, full_name='fn')
    assert resp == {**expected_base_item, **{'fullName': 'fn'}}

    resp = user_dynamo.set_user_details(
        user_id,
        full_name='f',
        display_name='d',
        bio='b',
        language_code='l',
        theme_code='tc',
        follow_counts_hidden=True,
        view_counts_hidden=True,
        email='e',
        phone='p',
        comments_disabled=True,
        likes_disabled=True,
        sharing_disabled=True,
        verification_hidden=True,
        date_of_birth=date_of_birth,
        gender=gender,
        location=location,
        height=height,
        match_age_range=match_age_range,
        match_genders=match_genders,
        match_location_radius=match_location_radius,
        match_height_range=match_height_range,
    )
    expected = {
        **expected_base_item,
        **{
            'fullName': 'f',
            'displayName': 'd',
            'bio': 'b',
            'languageCode': 'l',
            'themeCode': 'tc',
            'followCountsHidden': True,
            'viewCountsHidden': True,
            'email': 'e',
            'phoneNumber': 'p',
            'commentsDisabled': True,
            'likesDisabled': True,
            'sharingDisabled': True,
            'verificationHidden': True,
            'gender': gender,
            'location': location,
            'height': height,
            'matchAgeRange': match_age_range,
            'matchGenders': match_genders,
            'matchLocationRadius': match_location_radius,
            'matchHeightRange': match_height_range,
            'dateOfBirth': date_of_birth,
            'gsiK2PartitionKey': 'userBirthday/01-01',
            'gsiK2SortKey': '-',
        },
    }
    assert resp == expected

    # assert if accuracy is not set
    location = {'latitude': 50, 'longitude': 50}
    resp = user_dynamo.set_user_details(
        user_id,
        location=location,
    )
    assert 'accuracy' not in resp['location']


def test_set_user_details_delete_for_empty_string(user_dynamo):
    user_id = 'my-user-id'
    username = 'my-username'

    # create the user
    expected_base_item = user_dynamo.add_user(user_id, username)
    assert expected_base_item['userId'] == user_id

    # set all optionals
    resp = user_dynamo.set_user_details(
        user_id,
        full_name='f',
        display_name='d',
        bio='b',
        language_code='l',
        theme_code='tc',
        follow_counts_hidden=True,
        view_counts_hidden=True,
        email='e',
        phone='p',
        comments_disabled=True,
        likes_disabled=True,
        sharing_disabled=True,
        verification_hidden=True,
    )
    assert resp == {
        **expected_base_item,
        **{
            'fullName': 'f',
            'displayName': 'd',
            'bio': 'b',
            'languageCode': 'l',
            'themeCode': 'tc',
            'followCountsHidden': True,
            'viewCountsHidden': True,
            'email': 'e',
            'phoneNumber': 'p',
            'commentsDisabled': True,
            'likesDisabled': True,
            'sharingDisabled': True,
            'verificationHidden': True,
        },
    }

    # False does not mean delete anymore
    resp = user_dynamo.set_user_details(
        user_id,
        follow_counts_hidden=False,
        view_counts_hidden=False,
        comments_disabled=False,
        likes_disabled=False,
        sharing_disabled=False,
        verification_hidden=False,
    )
    assert resp == {
        **expected_base_item,
        **{
            'fullName': 'f',
            'displayName': 'd',
            'bio': 'b',
            'languageCode': 'l',
            'themeCode': 'tc',
            'followCountsHidden': False,
            'viewCountsHidden': False,
            'email': 'e',
            'phoneNumber': 'p',
            'commentsDisabled': False,
            'likesDisabled': False,
            'sharingDisabled': False,
            'verificationHidden': False,
        },
    }

    # empty string means delete
    resp = user_dynamo.set_user_details(
        user_id,
        full_name='',
        display_name='',
        bio='',
        language_code='',
        theme_code='',
        follow_counts_hidden='',
        view_counts_hidden='',
        email='',
        phone='',
        comments_disabled='',
        likes_disabled='',
        sharing_disabled='',
        verification_hidden='',
    )
    assert resp == expected_base_item


def test_set_user_details_date_of_birth(user_dynamo):
    user_id, username = str(uuid4()), str(uuid4())[:8]

    # create the user
    item = user_dynamo.add_user(user_id, username)
    assert user_dynamo.get_user(user_id) == item
    assert 'dateOfBirth' not in item
    assert 'gsiK2PartitionKey' not in item
    assert 'gsiK2SortKey' not in item

    # add a date of birth
    item = user_dynamo.set_user_details(user_id, date_of_birth='1999-12-31')
    assert user_dynamo.get_user(user_id) == item
    assert item['dateOfBirth'] == '1999-12-31'
    assert item['gsiK2PartitionKey'] == 'userBirthday/12-31'
    assert item['gsiK2SortKey'] == '-'

    # change date of birth
    item = user_dynamo.set_user_details(user_id, date_of_birth='2001-05-05')
    assert user_dynamo.get_user(user_id) == item
    assert item['dateOfBirth'] == '2001-05-05'
    assert item['gsiK2PartitionKey'] == 'userBirthday/05-05'
    assert item['gsiK2SortKey'] == '-'

    # remove date of birth
    item = user_dynamo.set_user_details(user_id, date_of_birth='')
    assert user_dynamo.get_user(user_id) == item
    assert 'dateOfBirth' not in item
    assert 'gsiK2PartitionKey' not in item
    assert 'gsiK2SortKey' not in item


def test_cant_set_privacy_status_to_random_string(user_dynamo):
    with pytest.raises(AssertionError, match='privacy_status'):
        user_dynamo.set_user_privacy_status('user-id', privacy_status='invalid')


def test_set_user_accepted_eula_version(user_dynamo):
    user_id = 'my-user-id'
    username = 'my-username'

    # create the user, verify user starts with no EULA version
    user_item = user_dynamo.add_user(user_id, username)
    assert user_item['userId'] == user_id
    assert 'acceptedEULAVersion' not in user_item

    # set it
    version_1 = '2019-11-14'
    user_item = user_dynamo.set_user_accepted_eula_version(user_id, version_1)
    assert user_item['acceptedEULAVersion'] == version_1

    # set it again
    version_2 = '2019-12-14'
    user_item = user_dynamo.set_user_accepted_eula_version(user_id, version_2)
    assert user_item['acceptedEULAVersion'] == version_2

    # delete it
    user_item = user_dynamo.set_user_accepted_eula_version(user_id, None)
    assert 'acceptedEULAVersion' not in user_item


def test_set_user_status(user_dynamo):
    # create the user, verify user starts as ACTIVE as default
    user_id = 'my-user-id'
    user_item = user_dynamo.add_user(user_id, 'thebestuser')
    assert user_item['userId'] == user_id
    assert 'userStatus' not in user_item
    assert 'lastDisabledAt' not in user_item

    # can't set it to an invalid value
    with pytest.raises(AssertionError, match='Invalid UserStatus'):
        user_dynamo.set_user_status(user_id, 'nopenope')

    # set it, check
    item = user_dynamo.set_user_status(user_id, UserStatus.DELETING)
    assert item['userStatus'] == UserStatus.DELETING
    assert 'lastDisabledAt' not in item

    # set it to DISABLED, check
    now = pendulum.now('utc')
    item = user_dynamo.set_user_status(user_id, UserStatus.DISABLED, now=now)
    assert item['userStatus'] == UserStatus.DISABLED
    assert item['lastDisabledAt'] == now.to_iso8601_string()

    # double check our writes really have been saving in the DB
    item = user_dynamo.get_user(user_id)
    assert item['userStatus'] == UserStatus.DISABLED
    assert item['lastDisabledAt'] == now.to_iso8601_string()

    # set it to DISABLED again, check
    item = user_dynamo.set_user_status(user_id, UserStatus.DISABLED)
    assert item['userStatus'] == UserStatus.DISABLED
    last_disabled_at = pendulum.parse(item['lastDisabledAt'])
    assert last_disabled_at > now

    # set it to the default, check
    item = user_dynamo.set_user_status(user_id, UserStatus.ACTIVE)
    assert 'userStatus' not in item
    assert item['lastDisabledAt'] == last_disabled_at.to_iso8601_string()


def test_set_user_privacy_status(user_dynamo):
    user_id = 'my-user-id'
    username = 'my-username'

    # create the user, verify user starts with PUBLIC
    user_item = user_dynamo.add_user(user_id, username)
    assert user_item['userId'] == user_id
    assert user_item['privacyStatus'] == UserPrivacyStatus.PUBLIC

    # set to private
    user_item = user_dynamo.set_user_privacy_status(user_id, UserPrivacyStatus.PRIVATE)
    assert user_item['privacyStatus'] == UserPrivacyStatus.PRIVATE

    # back to public
    user_item = user_dynamo.set_user_privacy_status(user_id, UserPrivacyStatus.PUBLIC)
    assert user_item['privacyStatus'] == UserPrivacyStatus.PUBLIC


def test_set_last_client(user_dynamo):
    user_id = str(uuid4())

    # create the user, verify user starts with no client info
    user_item = user_dynamo.add_user(user_id, 'my-username')
    assert user_item['userId'] == user_id
    assert 'lastClient' not in user_item

    # set some client info, verify
    client_1 = {
        'version': 'v2001',
        'system': 'one device to rule them all',
    }
    user_item = user_dynamo.set_last_client(user_id, client_1)
    assert user_dynamo.get_user(user_id) == user_item
    assert user_item['lastClient'] == client_1

    # update to some new client info, verify
    client_2 = {
        'system': 'and in the darkness use my camera flash to disable them',
        'version': 'v2022',
        'some-other-useful-data': 42,
    }
    user_item = user_dynamo.set_last_client(user_id, client_2)
    assert user_dynamo.get_user(user_id) == user_item
    assert user_item['lastClient'] == client_2


@pytest.mark.parametrize(
    'incrementor_name, decrementor_name, attribute_name',
    [
        ['increment_album_count', 'decrement_album_count', 'albumCount'],
        ['increment_card_count', 'decrement_card_count', 'cardCount'],
        ['increment_chat_count', 'decrement_chat_count', 'chatCount'],
        ['increment_chat_messages_creation_count', None, 'chatMessagesCreationCount'],
        ['increment_chat_messages_deletion_count', None, 'chatMessagesDeletionCount'],
        ['increment_chat_messages_forced_deletion_count', None, 'chatMessagesForcedDeletionCount'],
        [
            'increment_chats_with_unviewed_messages_count',
            'decrement_chats_with_unviewed_messages_count',
            'chatsWithUnviewedMessagesCount',
        ],
        ['increment_comment_count', 'decrement_comment_count', 'commentCount'],
        ['increment_comment_deleted_count', None, 'commentDeletedCount'],
        ['increment_comment_forced_deletion_count', None, 'commentForcedDeletionCount'],
        ['increment_followed_count', 'decrement_followed_count', 'followedCount'],
        ['increment_follower_count', 'decrement_follower_count', 'followerCount'],
        ['increment_followers_requested_count', 'decrement_followers_requested_count', 'followersRequestedCount'],
        ['increment_post_count', 'decrement_post_count', 'postCount'],
        ['increment_post_archived_count', 'decrement_post_archived_count', 'postArchivedCount'],
        ['increment_post_forced_archiving_count', None, 'postForcedArchivingCount'],
        ['increment_post_deleted_count', None, 'postDeletedCount'],
        ['increment_post_viewed_by_count', 'decrement_post_viewed_by_count', 'postViewedByCount'],
    ],
)
def test_increment_decrement_count(user_dynamo, caplog, incrementor_name, decrementor_name, attribute_name):
    incrementor = getattr(user_dynamo, incrementor_name)
    decrementor = getattr(user_dynamo, decrementor_name) if decrementor_name else None
    user_id = str(uuid4())

    # can't increment comment that doesnt exist
    with caplog.at_level(logging.WARNING):
        assert incrementor(user_id) is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(x in caplog.records[0].msg for x in ['Failed to increment', attribute_name, user_id])
    caplog.clear()

    # can't decrement comment that doesnt exist
    if decrementor:
        with caplog.at_level(logging.WARNING):
            assert decrementor(user_id) is None
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == 'WARNING'
        assert all(x in caplog.records[0].msg for x in ['Failed to decrement', attribute_name, user_id])
        caplog.clear()

    # add the user to the DB, verify it is in DB
    user_dynamo.add_user(user_id, str(uuid4())[:8])
    assert attribute_name not in user_dynamo.get_user(user_id)

    # increment twice, verify
    assert incrementor(user_id)[attribute_name] == 1
    assert user_dynamo.get_user(user_id)[attribute_name] == 1
    assert incrementor(user_id)[attribute_name] == 2
    assert user_dynamo.get_user(user_id)[attribute_name] == 2

    # all done if there's no decrementor method
    if not decrementor:
        return

    # decrement twice, verify
    assert decrementor(user_id)[attribute_name] == 1
    assert user_dynamo.get_user(user_id)[attribute_name] == 1
    assert decrementor(user_id)[attribute_name] == 0
    assert user_dynamo.get_user(user_id)[attribute_name] == 0

    # verify fail soft on trying to decrement below zero
    with caplog.at_level(logging.WARNING):
        assert decrementor(user_id) is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(x in caplog.records[0].msg for x in ['Failed to decrement', attribute_name, user_id])
    assert user_dynamo.get_user(user_id)[attribute_name] == 0


def test_grant_and_clear_subscription(user_dynamo):
    user_id = str(uuid4())
    assert user_dynamo.get_user(user_id) is None
    granted_at = pendulum.now('utc')
    expires_at = granted_at + pendulum.duration(days=3)  # arbitrary duration

    # Verify can't grant subscription to user that DNE
    # Note that this raises the wrong exception upon user DNE, but this isn't really an issue in
    # the app because when this method is called the user has already been verified to exist.
    with pytest.raises(UserAlreadyGrantedSubscription):
        user_dynamo.update_subscription(user_id, UserSubscriptionLevel.DIAMOND, granted_at, expires_at)
    assert user_dynamo.get_user(user_id) is None

    # verify can't clear subscription from user that DNE
    with pytest.raises(user_dynamo.client.exceptions.ConditionalCheckFailedException):
        user_dynamo.clear_subscription(user_id)
    assert user_dynamo.get_user(user_id) is None

    # create the user item
    user_item = user_dynamo.add_user(user_id, str(uuid4())[:8])
    assert user_dynamo.get_user(user_id) == user_item
    assert 'subscriptionLevel' not in user_item
    assert 'subscriptionGrantedAt' not in user_item
    assert 'subscriptionExpiresAt' not in user_item
    assert 'gsiK1PartitionKey' not in user_item
    assert 'gsiK1SortKey' not in user_item

    # verify we can't grant basic subscriptions
    with pytest.raises(AssertionError, match='BASIC'):
        user_dynamo.update_subscription(user_id, UserSubscriptionLevel.BASIC, granted_at, expires_at)
    assert user_dynamo.get_user(user_id) == user_item

    # verify we can't grant non-expiring subscriptions
    with pytest.raises(AssertionError, match='expire'):
        user_dynamo.update_subscription(user_id, UserSubscriptionLevel.DIAMOND, granted_at, None)
    assert user_dynamo.get_user(user_id) == user_item

    # verify we can do a normal grant
    new_user_item = user_dynamo.update_subscription(
        user_id, UserSubscriptionLevel.DIAMOND, granted_at, expires_at
    )
    assert user_dynamo.get_user(user_id) == new_user_item
    assert new_user_item == {
        **user_item,
        'subscriptionLevel': UserSubscriptionLevel.DIAMOND,
        'subscriptionGrantedAt': granted_at.to_iso8601_string(),
        'subscriptionExpiresAt': expires_at.to_iso8601_string(),
        'gsiK1PartitionKey': f'user/{UserSubscriptionLevel.DIAMOND}',
        'gsiK1SortKey': expires_at.to_iso8601_string(),
    }

    # verify we cannot re-grant ourselves more subscription after granting once
    new_expires_at = expires_at + pendulum.duration(days=4)
    with pytest.raises(UserAlreadyGrantedSubscription):
        user_dynamo.update_subscription(user_id, UserSubscriptionLevel.DIAMOND, granted_at, new_expires_at)
    assert user_dynamo.get_user(user_id) == new_user_item

    # clear the subscription, verify result, verify idempotent
    new_user_item = user_dynamo.clear_subscription(user_id)
    assert user_dynamo.get_user(user_id) == new_user_item
    assert new_user_item == {
        **user_item,
        'subscriptionGrantedAt': granted_at.to_iso8601_string(),
    }
    assert user_dynamo.clear_subscription(user_id) == new_user_item
    assert user_dynamo.get_user(user_id) == new_user_item

    # check that we can't re-grant ourselves another bonus subscription
    with pytest.raises(UserAlreadyGrantedSubscription):
        user_dynamo.update_subscription(user_id, UserSubscriptionLevel.DIAMOND, granted_at, new_expires_at)
    assert user_dynamo.get_user(user_id) == new_user_item


def test_update_subscription_not_a_grant(user_dynamo):
    user_id = str(uuid4())

    # Verify can't update subscription for user that DNE
    # Note that this raises the wrong exception upon user DNE, but this isn't really an issue in
    # the app because when this method is called the user has already been verified to exist.
    with pytest.raises(UserAlreadyGrantedSubscription):
        user_dynamo.update_subscription(user_id, UserSubscriptionLevel.DIAMOND)

    # verify we can't use this to go down to BASIC - use clear_subscription for that
    with pytest.raises(AssertionError, match='BASIC'):
        user_dynamo.update_subscription(user_id, UserSubscriptionLevel.BASIC)

    # verify we can't set an expires_at for non-grants: all non-grants auto-renew
    with pytest.raises(AssertionError, match='expire'):
        user_dynamo.update_subscription(user_id, UserSubscriptionLevel.DIAMOND, expires_at=pendulum.now('utc'))

    # create the user item
    user_item = user_dynamo.add_user(user_id, str(uuid4())[:8])
    assert user_dynamo.get_user(user_id) == user_item
    assert 'subscriptionLevel' not in user_item
    assert 'subscriptionGrantedAt' not in user_item
    assert 'subscriptionExpiresAt' not in user_item
    assert 'gsiK1PartitionKey' not in user_item
    assert 'gsiK1SortKey' not in user_item

    # success case
    user_dynamo.update_subscription(user_id, UserSubscriptionLevel.DIAMOND)
    assert user_dynamo.get_user(user_id) == {**user_item, 'subscriptionLevel': UserSubscriptionLevel.DIAMOND}

    # check idempotent
    user_dynamo.update_subscription(user_id, UserSubscriptionLevel.DIAMOND)
    assert user_dynamo.get_user(user_id) == {**user_item, 'subscriptionLevel': UserSubscriptionLevel.DIAMOND}


def test_generate_user_ids_by_subscription_level(user_dynamo):
    DIAMOND = UserSubscriptionLevel.DIAMOND
    ms = pendulum.duration(microseconds=1)

    # add a few users
    user_id_1, user_id_2, user_id_3 = str(uuid4()), str(uuid4()), str(uuid4())
    user_dynamo.add_user(user_id_1, str(uuid4())[:8])
    user_dynamo.add_user(user_id_2, str(uuid4())[:8])
    user_dynamo.add_user(user_id_3, str(uuid4())[:8])

    # give two of them subscriptions to diamond, give the third a distraction
    now = pendulum.now('utc')
    expires_at_1 = now + pendulum.duration(hours=1)
    expires_at_2 = now + pendulum.duration(hours=2)
    user_dynamo.update_subscription(user_id_1, DIAMOND, now, expires_at_1)
    user_dynamo.update_subscription(user_id_2, DIAMOND, now, expires_at_2)
    user_dynamo.update_subscription(user_id_3, 'distraction', now, expires_at_1)

    # test generate none, one, two, all
    generate = user_dynamo.generate_user_ids_by_subscription_level
    assert list(generate('something-else')) == []
    assert list(generate(DIAMOND, max_expires_at=expires_at_1 - ms)) == []
    assert list(generate(DIAMOND, max_expires_at=expires_at_1)) == [user_id_1]
    assert list(generate(DIAMOND, max_expires_at=expires_at_2 - ms)) == [user_id_1]
    assert list(generate(DIAMOND, max_expires_at=expires_at_2)) == [user_id_1, user_id_2]
    assert list(generate(DIAMOND)) == [user_id_1, user_id_2]


@pytest.mark.parametrize('view_type', [None, ViewType.THUMBNAIL, ViewType.FOCUS])
def test_update_last_post_view_at(user_dynamo, caplog, view_type):
    user_id = str(uuid4())
    user_dynamo.add_user(user_id, str(uuid4())[:8])
    assert 'lastPostViewAt' not in user_dynamo.get_user(user_id)
    assert 'lastPostFocusViewAt' not in user_dynamo.get_user(user_id)
    ms = pendulum.duration(microseconds=1)

    # set it without specifying time exactly, verify
    user_item = user_dynamo.update_last_post_view_at(user_id, view_type=view_type)
    assert user_dynamo.get_user(user_id) == user_item
    assert 'lastPostViewAt' in user_item
    now = pendulum.parse(user_item['lastPostViewAt'])
    if view_type == ViewType.FOCUS:
        assert user_item['lastPostFocusViewAt'] == user_item['lastPostViewAt']
    else:
        assert 'lastPostFocusViewAt' not in user_item

    # verify can't set it earlier time
    with caplog.at_level(logging.WARNING):
        user_dynamo.update_last_post_view_at(user_id, now=(now - ms), view_type=view_type)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(x in caplog.records[0].msg for x in ['Failed to update lastPostViewAt', user_id])

    # verify can set it to a later time
    user_item = user_dynamo.update_last_post_view_at(user_id, now=(now + ms), view_type=view_type)
    assert user_dynamo.get_user(user_id) == user_item
    assert pendulum.parse(user_item['lastPostViewAt']) == now + ms
    if view_type == ViewType.FOCUS:
        assert user_item['lastPostFocusViewAt'] == user_item['lastPostViewAt']
    else:
        assert 'lastPostFocusViewAt' not in user_item


def test_update_last_post_view_at_user_dne(user_dynamo, caplog):
    # verify setting it on a user that DNE fails softly
    user_id_2 = str(uuid4())
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        user_dynamo.update_last_post_view_at(user_id_2)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(x in caplog.records[0].msg for x in ['Failed to update lastPostViewAt', user_id_2])


def test_add_delete_user_deleted(user_dynamo, caplog):
    # verify starting state
    user_id = str(uuid4())
    key = {'partitionKey': f'user/{user_id}', 'sortKey': 'deleted'}
    assert user_dynamo.client.get_item(key) is None

    # add the item, verify
    before = pendulum.now('utc')
    user_deleted_item = user_dynamo.add_user_deleted(user_id)
    after = pendulum.now('utc')
    assert user_dynamo.client.get_item(key) == user_deleted_item
    deleted_at = pendulum.parse(user_deleted_item['deletedAt'])
    assert user_deleted_item == {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'deleted',
        'schemaVersion': 0,
        'userId': user_id,
        'deletedAt': deleted_at.to_iso8601_string(),
        'gsiA1PartitionKey': 'userDeleted',
        'gsiA1SortKey': deleted_at.to_iso8601_string(),
    }
    assert deleted_at >= before
    assert deleted_at <= after

    # verify can't add same subitem a second time
    with caplog.at_level(logging.WARNING):
        new_item = user_dynamo.add_user_deleted(user_id)
    assert new_item is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(x in caplog.records[0].msg for x in ['Failed to add UserDeleted subitem', user_id])

    # delete the subitem
    new_item = user_dynamo.delete_user_deleted(user_id)
    assert new_item == user_deleted_item
    assert user_dynamo.client.get_item(key) is None

    # verify deletes are idempotent
    new_item = user_dynamo.delete_user_deleted(user_id)
    assert new_item is None
    assert user_dynamo.client.get_item(key) is None

    # verify we can now re-add the subitem
    new_item = user_dynamo.add_user_deleted(user_id, now=deleted_at)
    assert user_dynamo.client.get_item(key) == new_item
    assert new_item == user_deleted_item


def test_set_user_age(user_dynamo):
    user_id = str(uuid4())

    # can't set for user that DNE
    with pytest.raises(user_dynamo.client.exceptions.ConditionalCheckFailedException):
        user_dynamo.set_user_age(user_id, 23)

    # add user to DB, verify starts without age
    item = user_dynamo.add_user(user_id, user_id[:8])
    assert user_dynamo.get_user(user_id) == item
    assert 'age' not in item

    # set it
    item = user_dynamo.set_user_age(user_id, 24)
    assert user_dynamo.get_user(user_id) == item
    assert item['age'] == 24

    # verify can't set it to a float that's not an int
    with pytest.raises(AssertionError):
        user_dynamo.set_user_age(user_id, 25.2)
    assert user_dynamo.get_user(user_id) == item
    assert item['age'] == 24

    # verify can use a float that is equal to an int
    item = user_dynamo.set_user_age(user_id, 26.0)
    assert user_dynamo.get_user(user_id) == item
    assert item['age'] == 26

    # verify can remove it
    item = user_dynamo.set_user_age(user_id, None)
    assert user_dynamo.get_user(user_id) == item
    assert 'age' not in item


def test_set_user_dating_status(user_dynamo):
    user_id = str(uuid4())

    # verify can't set for user that DNE
    with pytest.raises(user_dynamo.client.exceptions.ConditionalCheckFailedException):
        user_dynamo.set_user_dating_status(user_id, UserDatingStatus.ENABLED)

    # add user to DB, verify starts without dating status
    item = user_dynamo.add_user(user_id, user_id[:8])
    assert user_dynamo.get_user(user_id) == item
    assert 'datingStatus' not in item
    assert 'gsiA3PartitionKey' not in item
    assert 'gsiA3SortKey' not in item
    assert 'userDisableDatingDate' not in item

    # enable, verify
    item = user_dynamo.set_user_dating_status(user_id, UserDatingStatus.ENABLED)
    assert user_dynamo.get_user(user_id) == item
    assert item['datingStatus'] == UserDatingStatus.ENABLED
    assert item['gsiA3PartitionKey'] == 'userDisableDatingDate'
    assert item['gsiA3SortKey']
    assert 'userDisableDatingDate' not in item

    # disable, verify
    before = pendulum.now('utc')
    item = user_dynamo.set_user_dating_status(user_id, UserDatingStatus.DISABLED)
    after = pendulum.now('utc')

    assert user_dynamo.get_user(user_id) == item
    assert 'datingStatus' not in item
    assert 'gsiA3PartitionKey' not in item
    assert 'gsiA3SortKey' not in item
    assert before < pendulum.parse(item['userDisableDatingDate']) < after


def test_generate_user_ids_by_birthday(user_dynamo):
    uid1, uid2, uid3 = str(uuid4()), str(uuid4()), str(uuid4())
    user_dynamo.add_user(uid1, uid1[:8])
    user_dynamo.add_user(uid2, uid2[:8])
    user_dynamo.add_user(uid3, uid3[:8])
    user_dynamo.set_user_details(uid1, date_of_birth='1970-12-31')
    user_dynamo.set_user_details(uid2, date_of_birth='2000-02-29')
    user_dynamo.set_user_details(uid3, date_of_birth='2001-12-31')

    # test generate none, one, two
    assert list(user_dynamo.generate_user_ids_by_birthday('09-09')) == []
    assert list(user_dynamo.generate_user_ids_by_birthday('02-29')) == [uid2]
    assert list(user_dynamo.generate_user_ids_by_birthday('12-31')).sort() == [uid1, uid3].sort()


def test_set_user_last_found_contacts_at(user_dynamo):
    user_id = 'my-user-id'
    username = 'my-username'
    now = pendulum.now('utc')

    expected_base_item = user_dynamo.add_user(user_id, username)
    assert expected_base_item['userId'] == user_id

    # Check set_user_last_found_contacts_at without Specific Time
    before = pendulum.now('utc')
    resp = user_dynamo.set_user_last_found_contacts_at(user_id)
    after = pendulum.now('utc')

    assert before < pendulum.parse(resp['lastFoundContactsAt']) < after

    # Check set_user_last_found_contacts_at with Specific Time
    resp = user_dynamo.set_user_last_found_contacts_at(user_id, now)
    current_time = now.to_iso8601_string()

    assert current_time == resp['lastFoundContactsAt']


def test_set_last_disable_dating_date(user_dynamo):
    expired_at = (pendulum.now('utc') + pendulum.duration(days=30)).to_date_string()
    user_id = str(uuid4())

    # verify can't set for user that DNE
    with pytest.raises(user_dynamo.client.exceptions.ConditionalCheckFailedException):
        user_dynamo.set_last_disable_dating_date(user_id)

    # add user to DB, verify starts without last disable dating date
    item = user_dynamo.add_user(user_id, user_id[:8])
    assert user_dynamo.get_user(user_id) == item
    assert 'gsiA3PartitionKey' not in item
    assert 'gsiA3SortKey' not in item

    # set, verify
    item = user_dynamo.set_last_disable_dating_date(user_id)
    assert user_dynamo.get_user(user_id) == item
    assert item['gsiA3PartitionKey'] == 'userDisableDatingDate'
    assert item['gsiA3SortKey'] == expired_at


def test_generate_user_ids_by_expired_dating(user_dynamo):
    # add a few users
    user_id_1, user_id_2, user_id_3 = str(uuid4()), str(uuid4()), str(uuid4())
    user_dynamo.add_user(user_id_1, str(uuid4())[:8])
    user_dynamo.add_user(user_id_2, str(uuid4())[:8])
    user_dynamo.add_user(user_id_3, str(uuid4())[:8])

    # give two of them unexpired date, give the third a expired date

    user_dynamo.set_last_disable_dating_date(user_id_1)
    user_dynamo.set_last_disable_dating_date(user_id_2)
    user_dynamo.set_last_disable_dating_date(user_id_3)

    # test generate expired dating date user ids
    generate = user_dynamo.generate_user_ids_by_expired_dating
    now = pendulum.now('utc')
    now_1 = now + pendulum.duration(days=1)
    now_2 = now + pendulum.duration(days=10)
    now_3 = now + pendulum.duration(days=30)
    now_4 = now + pendulum.duration(days=32)

    assert list(generate(now)) == []
    assert list(generate(now_1)) == []
    assert list(generate(now_2)) == []
    assert list(generate(now_3)) == [user_id_1, user_id_2, user_id_3]
    assert list(generate(now_4)) == [user_id_1, user_id_2, user_id_3]


def test_add_user_banned(user_dynamo, caplog):
    # verify starting state
    user_id = str(uuid4())
    key = {'partitionKey': f'user/{user_id}', 'sortKey': 'banned'}
    assert user_dynamo.client.get_item(key) is None

    # add the item, verify
    before = pendulum.now('utc')
    user_banned_item = user_dynamo.add_user_banned(user_id, 'abc', 'comments', email='abc@test.com')
    after = pendulum.now('utc')
    assert user_dynamo.client.get_item(key) == user_banned_item
    banned_at = pendulum.parse(user_banned_item['bannedAt'])
    assert user_banned_item == {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'banned',
        'schemaVersion': 0,
        'userId': user_id,
        'username': 'abc',
        'bannedAt': banned_at.to_iso8601_string(),
        'forcedBy': 'comments',
        'gsiA1PartitionKey': 'email/abc@test.com',
        'gsiA1SortKey': 'banned',
    }
    assert banned_at >= before
    assert banned_at <= after

    # verify can't add same subitem a second time
    with caplog.at_level(logging.WARNING):
        new_item = user_dynamo.add_user_banned(user_id, 'abc', 'comments')
    assert new_item is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(x in caplog.records[0].msg for x in ['Failed to add UserBanned item', user_id])

    # delete the subitem
    new_item = user_dynamo.delete_user_banned(user_id)
    assert new_item == user_banned_item
    assert user_dynamo.client.get_item(key) is None

    # verify deletes are idempotent
    new_item = user_dynamo.delete_user_banned(user_id)
    assert new_item is None
    assert user_dynamo.client.get_item(key) is None


def test_generate_banned_user_by_contact_attr(user_dynamo, caplog):
    user_id_1, user_id_2, user_id_3 = str(uuid4()), str(uuid4()), str(uuid4())
    user_dynamo.add_user(user_id_1, str(uuid4())[:8])
    user_dynamo.add_user(user_id_2, str(uuid4())[:8])
    user_dynamo.add_user(user_id_3, str(uuid4())[:8])

    # add banned user

    user_dynamo.add_user_banned(user_id_1, 'abc-1', 'comments', email='abc1@test.com')
    user_dynamo.add_user_banned(user_id_2, 'abc-2', 'posts', phone='+1234567890')
    user_dynamo.add_user_banned(user_id_3, 'abc-3', 'chatMessages', device='x-real-device-1')

    user_ids = user_dynamo.generate_banned_user_by_contact_attr(email='abc1@test.com')
    assert user_ids == [user_id_1]
    user_ids = user_dynamo.generate_banned_user_by_contact_attr(phone='+1234567890')
    assert user_ids == [user_id_2]
    user_ids = user_dynamo.generate_banned_user_by_contact_attr(device='x-real-device-1')
    assert user_ids == [user_id_3]

    user_ids = user_dynamo.generate_banned_user_by_contact_attr(device='x-real-device')
    assert user_ids == []
    user_ids = user_dynamo.generate_banned_user_by_contact_attr(email='abc@test.com')
    assert user_ids == []
    user_ids = user_dynamo.generate_banned_user_by_contact_attr(phone='1234567890')
    assert user_ids == []


def test_generate_dating_enabled_user_ids(user_dynamo):
    user1_id = str(uuid4())
    user2_id = str(uuid4())
    user3_id = str(uuid4())

    # add user to DB, verify starts without dating status
    item = user_dynamo.add_user(user1_id, user1_id[:8])
    assert user_dynamo.get_user(user1_id) == item
    item = user_dynamo.add_user(user2_id, user2_id[:8])
    assert user_dynamo.get_user(user2_id) == item
    item = user_dynamo.add_user(user3_id, user3_id[:8])
    assert user_dynamo.get_user(user3_id) == item

    # enable, verify
    user_dynamo.set_user_dating_status(user1_id, UserDatingStatus.ENABLED)
    user_dynamo.set_user_dating_status(user2_id, UserDatingStatus.ENABLED)
    user_dynamo.set_user_dating_status(user3_id, UserDatingStatus.ENABLED)

    assert list(user_dynamo.generate_dating_enabled_user_ids()) == [user1_id, user2_id, user3_id]

    # disable user3
    user_dynamo.set_user_dating_status(user3_id, UserDatingStatus.DISABLED)

    assert list(user_dynamo.generate_dating_enabled_user_ids()) == [user1_id, user2_id]
