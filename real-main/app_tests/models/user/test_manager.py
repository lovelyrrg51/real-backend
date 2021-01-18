import re
import uuid
from decimal import Decimal
from unittest import mock

import pendulum
import pytest

from app.models.follower.enums import FollowStatus
from app.models.user.enums import UserDatingStatus, UserGender
from app.utils import GqlNotificationType


@pytest.fixture
def cognito_only_user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def cognito_only_user_with_phone(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_phone='+12125551212')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def cognito_only_user_with_email_and_phone(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(
        user_id, username, verified_email=f'{username}@real.app', verified_phone='+12125551213'
    )
    yield user_manager.create_cognito_only_user(user_id, username)


user1 = cognito_only_user
user2 = cognito_only_user
user3 = cognito_only_user
user4 = cognito_only_user_with_phone
user5 = cognito_only_user_with_email_and_phone
user6 = cognito_only_user


@pytest.fixture
def real_user(user_manager, cognito_client):
    user_id = str(uuid.uuid4())
    cognito_client.create_user_pool_entry(user_id, 'real', verified_email='real-test@real.app')
    yield user_manager.create_cognito_only_user(user_id, 'real')


def test_get_user_that_doesnt_exist(user_manager):
    resp = user_manager.get_user('nope-not-there')
    assert resp is None


def test_get_user_by_username(user_manager, user1):
    # check a user that doesn't exist
    user = user_manager.get_user_by_username('nope_not_there')
    assert user is None

    # check a user that exists
    user = user_manager.get_user_by_username(user1.username)
    assert user.id == user1.id


def test_generate_username(user_manager):
    for _ in range(10):
        username = user_manager.generate_username()
        user_manager.validate_username(username)  # should not raise exception


def test_follow_real_user_exists(user_manager, user1, follower_manager, real_user):
    # verify no followers (ensures user1 fixture generated before real_user)
    assert list(follower_manager.dynamo.generate_followed_items(user1.id)) == []

    # follow that real user
    user_manager.follow_real_user(user1)
    followeds = list(follower_manager.dynamo.generate_followed_items(user1.id))
    assert len(followeds) == 1
    assert followeds[0]['followedUserId'] == real_user.id


def test_follow_real_user_doesnt_exist(user_manager, user1, follower_manager):
    assert list(follower_manager.dynamo.generate_followed_items(user1.id)) == []
    user_manager.follow_real_user(user1)
    assert list(follower_manager.dynamo.generate_followed_items(user1.id)) == []


def test_get_available_placeholder_photo_codes(user_manager):
    s3_client = user_manager.s3_placeholder_photos_client
    user_manager.placeholder_photos_directory = 'placeholder-photos'

    # check before we add any placeholder photos
    codes = user_manager.get_available_placeholder_photo_codes()
    assert codes == []

    # add a placeholder photo, check again
    path = 'placeholder-photos/black-white-cat/native.jpg'
    s3_client.put_object(path, b'placeholder', 'image/jpeg')
    codes = user_manager.get_available_placeholder_photo_codes()
    assert len(codes) == 1
    assert codes[0] == 'black-white-cat'

    # add another placeholder photo, check again
    path = 'placeholder-photos/orange-person/native.jpg'
    s3_client.put_object(path, b'placeholder', 'image/jpeg')
    path = 'placeholder-photos/orange-person/4k.jpg'
    s3_client.put_object(path, b'placeholder', 'image/jpeg')
    codes = user_manager.get_available_placeholder_photo_codes()
    assert len(codes) == 2
    assert codes[0] == 'black-white-cat'
    assert codes[1] == 'orange-person'


def test_get_random_placeholder_photo_code(user_manager):
    s3_client = user_manager.s3_placeholder_photos_client
    user_manager.placeholder_photos_directory = 'placeholder-photos'

    # check before we add any placeholder photos
    code = user_manager.get_random_placeholder_photo_code()
    assert code is None

    # add a placeholder photo, check again
    path = 'placeholder-photos/black-white-cat/native.jpg'
    s3_client.put_object(path, b'placeholder', 'image/jpeg')
    code = user_manager.get_random_placeholder_photo_code()
    assert code == 'black-white-cat'

    # add another placeholder photo, check again
    path = 'placeholder-photos/orange-person/native.jpg'
    s3_client.put_object(path, b'placeholder', 'image/jpeg')
    path = 'placeholder-photos/orange-person/4k.jpg'
    s3_client.put_object(path, b'placeholder', 'image/jpeg')
    code = user_manager.get_random_placeholder_photo_code()
    assert code in ['black-white-cat', 'orange-person']


def test_get_text_tags(user_manager, user1, user2):
    # no tags
    text = 'no tags here'
    assert user_manager.get_text_tags(text) == []

    # with tags, but not of users that exist
    text = 'hey @youDontExist and @meneither'
    assert user_manager.get_text_tags(text) == []

    # with tags, some that exist and others that dont
    text = f'hey @{user1.username} and @nopenope and @{user2.username}'
    assert sorted(user_manager.get_text_tags(text), key=lambda x: x['tag']) == sorted(
        [{'tag': f'@{user1.username}', 'userId': user1.id}, {'tag': f'@{user2.username}', 'userId': user2.id}],
        key=lambda x: x['tag'],
    )


def test_username_tag_regex(user_manager):
    reg = user_manager.username_tag_regex

    # no tags
    assert re.findall(reg, '') == []
    assert re.findall(reg, 'no tags here') == []

    # basic tags
    assert re.findall(reg, 'hi @you how @are @you') == ['@you', '@are', '@you']
    assert re.findall(reg, 'hi @y3o@m.e@ever_yone') == ['@y3o', '@m.e', '@ever_yone']

    # near misses
    assert re.findall(reg, 'too @34 @.. @go!forit @no-no') == []

    # uglies
    assert re.findall(reg, 'hi @._._ @4_. @A_A\n@B.4\r@333!?') == ['@._._', '@4_.', '@A_A', '@B.4', '@333']


def test_clear_expired_subscriptions(user_manager, user1, user2, user3):
    sub_duration = pendulum.duration(months=1)
    ms = pendulum.duration(microseconds=1)

    # grant these users subscriptions that expire at different times, verify
    now1 = pendulum.now('utc')
    user1.grant_subscription_bonus(now=now1)
    user2.grant_subscription_bonus(now=now1 + pendulum.duration(hours=1))
    user3.grant_subscription_bonus(now=now1 + pendulum.duration(hours=2))
    assert user1.refresh_item().item['subscriptionLevel']
    assert user2.refresh_item().item['subscriptionLevel']
    assert user3.refresh_item().item['subscriptionLevel']

    # test clear none
    assert user_manager.clear_expired_subscriptions() == 0
    assert user_manager.clear_expired_subscriptions(now=now1 + sub_duration - ms) == 0
    assert user1.refresh_item().item['subscriptionLevel']
    assert user2.refresh_item().item['subscriptionLevel']
    assert user3.refresh_item().item['subscriptionLevel']

    # test clear one of them
    assert user_manager.clear_expired_subscriptions(now=now1 + sub_duration) == 1
    assert 'subscriptionLevel' not in user1.refresh_item().item
    assert user2.refresh_item().item['subscriptionLevel']
    assert user3.refresh_item().item['subscriptionLevel']

    # test clear two of them
    assert user_manager.clear_expired_subscriptions(now=now1 + sub_duration + pendulum.duration(hours=2)) == 2
    assert 'subscriptionLevel' not in user1.refresh_item().item
    assert 'subscriptionLevel' not in user2.refresh_item().item
    assert 'subscriptionLevel' not in user3.refresh_item().item


def test_fire_gql_subscription_chats_with_unviewed_messages_count(user_manager):
    user_id = str(uuid.uuid4())
    user_item = {'chatsWithUnviewedMessagesCount': Decimal(34), 'otherField': 'anything'}
    with mock.patch.object(user_manager, 'appsync_client') as appsync_client_mock:
        user_manager.fire_gql_subscription_chats_with_unviewed_messages_count(user_id, user_item, 'unused')
    assert appsync_client_mock.mock_calls == [
        mock.call.fire_notification(
            user_id,
            GqlNotificationType.USER_CHATS_WITH_UNVIEWED_MESSAGES_COUNT_CHANGED,
            userChatsWithUnviewedMessagesCount=34,
        )
    ]
    # Decimals cause problems when serializing to JSON so make sure we've converted to int
    assert isinstance(
        appsync_client_mock.fire_notification.call_args.kwargs['userChatsWithUnviewedMessagesCount'], int
    )


def test_find_user_finds_correct_users(user_manager, user1, user2, user4, user5):
    # Add contact attribute subitem for user2's email
    user_manager.on_user_email_change_update_subitem(user2.id, new_item=user2.item)

    # Add contact attribute subitem for user4's phone
    user_manager.on_user_phone_number_change_update_subitem(user4.id, new_item=user4.item)

    # Add contact attribute subitem for user5's phone & email
    user_manager.on_user_email_change_update_subitem(user5.id, new_item=user5.item)
    user_manager.on_user_phone_number_change_update_subitem(user5.id, new_item=user5.item)

    # Check with None
    assert user_manager.find_contacts(user1, contacts=[{'contactId': str(uuid.uuid4())}]) == {}

    # Check with only email
    contacts = [
        {
            'contactId': 'id_contact_1',
            'emails': [user2.item['email']],
        },
        {
            'contactId': 'id_contact_2',
            'emails': [user5.item['email']],
        },
    ]
    response = {'id_contact_1': user2.id, 'id_contact_2': user5.id}

    contact_id_to_user_id = user_manager.find_contacts(user1, contacts=contacts)
    for contact_id, user_id in contact_id_to_user_id.items():
        assert response[contact_id] == user_id

    # Check with only phone
    contacts = [
        {
            'contactId': 'id_contact_1',
            'phones': [user4.item['phoneNumber']],
        },
        {
            'contactId': 'id_contact_2',
            'phones': [user5.item['phoneNumber']],
        },
    ]
    response = {'id_contact_1': user4.id, 'id_contact_2': user5.id}

    contact_id_to_user_id = user_manager.find_contacts(user1, contacts=contacts)
    for contact_id, user_id in contact_id_to_user_id.items():
        assert response[contact_id] == user_id

    # Check with phone & email
    contacts = [
        {
            'contactId': 'id_contact_1',
            'emails': [user2.item['email']],
        },
        {
            'contactId': 'id_contact_2',
            'phones': [user4.item['phoneNumber']],
        },
        {
            'contactId': 'id_contact_3',
            'emails': [user5.item['email']],
            'phones': [user5.item['phoneNumber']],
        },
    ]
    response = {'id_contact_1': user2.id, 'id_contact_2': user4.id, 'id_contact_3': user5.id}

    contact_id_to_user_id = user_manager.find_contacts(user1, contacts=contacts)
    for contact_id, user_id in contact_id_to_user_id.items():
        assert response[contact_id] == user_id


def test_find_user_add_cards_for_found_users_not_following(user_manager, user1, user2, user3, user5):
    follower_manager = user_manager.follower_manager
    card_manager = user_manager.card_manager

    # Add contact attribute subitems for users emails
    user_manager.on_user_email_change_update_subitem(user2.id, new_item=user2.item)
    user_manager.on_user_email_change_update_subitem(user3.id, new_item=user3.item)
    user_manager.on_user_email_change_update_subitem(user5.id, new_item=user5.item)

    # verify user2, user3 and user5 don't have cards for user1 already
    card_id2 = f'{user2.id}:CONTACT_JOINED:{user1.id}'
    card_id3 = f'{user3.id}:CONTACT_JOINED:{user1.id}'
    card_id5 = f'{user5.id}:CONTACT_JOINED:{user1.id}'
    assert card_manager.get_card(card_id2) is None
    assert card_manager.get_card(card_id3) is None
    assert card_manager.get_card(card_id5) is None

    # set up user3 to follow user1
    follower_manager.request_to_follow(user3, user1)
    assert follower_manager.get_follow_status(user3.id, user1.id) == FollowStatus.FOLLOWING

    # user1 finds all three users using their email, verify users that are not following get cards
    contacts = [
        {
            'contactId': 'id_contact_1',
            'emails': [user2.item['email']],
        },
        {
            'contactId': 'id_contact_2',
            'emails': [user3.item['email']],
        },
        {
            'contactId': 'id_contact_3',
            'emails': [user5.item['email']],
        },
    ]
    user_manager.find_contacts(user1, contacts=contacts)
    assert card_manager.get_card(card_id2)
    assert card_manager.get_card(card_id3) is None
    assert card_manager.get_card(card_id5)


def test_update_ages(user_manager, user1, user2, user3, user6):
    # set birthdates for three of the users, leave one blank
    user1.update_details(date_of_birth='1990-07-01')
    user2.update_details(date_of_birth='2000-07-02')
    user3.update_details(date_of_birth='2010-07-01')
    assert 'age' not in user1.refresh_item().item
    assert 'age' not in user2.refresh_item().item
    assert 'age' not in user3.refresh_item().item
    assert 'age' not in user6.refresh_item().item

    now = pendulum.parse('2020-06-30T23:59:59Z')
    assert user_manager.update_ages(now=now) == (0, 0)
    assert 'age' not in user1.refresh_item().item
    assert 'age' not in user2.refresh_item().item
    assert 'age' not in user3.refresh_item().item
    assert 'age' not in user6.refresh_item().item

    now = pendulum.parse('2020-07-01T06:59:59Z')
    assert user_manager.update_ages(now=now) == (2, 2)
    assert user1.refresh_item().item['age'] == 30
    assert 'age' not in user2.refresh_item().item
    assert user3.refresh_item().item['age'] == 10
    assert 'age' not in user6.refresh_item().item

    now = pendulum.parse('2020-07-01T18:59:59Z')
    assert user_manager.update_ages(now=now) == (2, 0)
    assert user1.refresh_item().item['age'] == 30
    assert 'age' not in user2.refresh_item().item
    assert user3.refresh_item().item['age'] == 10
    assert 'age' not in user6.refresh_item().item

    now = pendulum.parse('2020-07-02T06:59:59Z')
    assert user_manager.update_ages(now=now) == (1, 1)
    assert user1.refresh_item().item['age'] == 30
    assert user2.refresh_item().item['age'] == 20
    assert user3.refresh_item().item['age'] == 10
    assert 'age' not in user6.refresh_item().item


def test_clear_expired_dating_status(user_manager, user1, user2, user3):
    # set last dating date
    user1.item['datingStatus'] = UserDatingStatus.ENABLED
    user2.item['datingStatus'] = UserDatingStatus.ENABLED
    user3.item['datingStatus'] = UserDatingStatus.ENABLED
    user1.set_last_disable_dating_date()
    user2.set_last_disable_dating_date()
    user3.set_last_disable_dating_date()

    assert user1.refresh_item().item['gsiA3SortKey']
    assert user2.refresh_item().item['gsiA3SortKey']
    assert user3.refresh_item().item['gsiA3SortKey']

    now = pendulum.now('utc')
    assert user_manager.clear_expired_dating_status(now=now) == 0
    assert user1.refresh_item().item['gsiA3SortKey']
    assert user2.refresh_item().item['gsiA3SortKey']
    assert user3.refresh_item().item['gsiA3SortKey']

    now = pendulum.now('utc') + pendulum.duration(days=29)
    assert user_manager.clear_expired_dating_status(now=now) == 0
    assert user1.refresh_item().item['gsiA3SortKey']
    assert user2.refresh_item().item['gsiA3SortKey']
    assert user3.refresh_item().item['gsiA3SortKey']

    # after 30 days, dating status set to disable
    now = pendulum.now('utc') + pendulum.duration(days=30)
    assert user_manager.clear_expired_dating_status(now=now) == 3
    assert 'datingStatus' not in user1.refresh_item().item
    assert 'datingStatus' not in user2.refresh_item().item
    assert 'datingStatus' not in user3.refresh_item().item
    assert 'gsiA3SortKey' not in user1.refresh_item().item
    assert 'gsiA3SortKey' not in user2.refresh_item().item
    assert 'gsiA3SortKey' not in user3.refresh_item().item


def test_send_dating_matches_notification(user_manager, user1, user2):
    # enable dating
    user1.dynamo.set_user_photo_post_id(user1.id, str(uuid.uuid4()))
    user1.update_details(
        full_name='grant',
        display_name='grant',
        date_of_birth='2000-01-01',
        gender=UserGender.FEMALE,
        location={'latitude': 45, 'longitude': -120},
        height=90,
        match_age_range={'min': 20, 'max': 30},
        match_genders=[UserGender.FEMALE, UserGender.MALE],
        match_location_radius=50,
        match_height_range={'min': 50, 'max': 110},
    )
    user1.update_age(now=pendulum.parse('2020-02-02T00:00:00Z'))
    user1.set_dating_status(UserDatingStatus.ENABLED)

    user2.dynamo.set_user_photo_post_id(user2.id, str(uuid.uuid4()))
    user2.update_details(
        full_name='grant',
        display_name='grant',
        date_of_birth='2000-01-01',
        gender=UserGender.FEMALE,
        location={'latitude': 45, 'longitude': -120},
        height=90,
        match_age_range={'min': 20, 'max': 30},
        match_genders=[UserGender.FEMALE, UserGender.MALE],
        match_location_radius=50,
        match_height_range={'min': 50, 'max': 110},
    )
    user2.update_age(now=pendulum.parse('2020-02-02T00:00:00Z'))
    user2.set_dating_status(UserDatingStatus.ENABLED)

    card_manager = user_manager.card_manager
    card_id1 = f'{user1.id}:USER_DATING_NEW_MATCHES'
    card_id2 = f'{user2.id}:USER_DATING_NEW_MATCHES'
    assert card_manager.get_card(card_id1) is None
    assert card_manager.get_card(card_id2) is None
