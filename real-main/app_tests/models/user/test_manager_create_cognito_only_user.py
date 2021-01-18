from unittest import mock
from uuid import uuid4

import pytest

from app.models.user.exceptions import UserAlreadyExists, UserValidationException


@pytest.fixture
def cognito_only_user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def real_user(user_manager, cognito_client):
    user_id = str(uuid4())
    cognito_client.create_user_pool_entry(user_id, 'real', verified_email='real-test@real.app')
    yield user_manager.create_cognito_only_user(user_id, 'real')


def test_create_cognito_user(user_manager, cognito_client):
    user_id = 'my-user-id'
    username = 'myusername'
    full_name = 'my-full-name'
    email = f'{username}@real.app'

    # check the user doesn't already exist
    user = user_manager.get_user(user_id)
    assert user is None

    # frontend does this part out-of-band
    cognito_client.create_user_pool_entry(user_id, username, verified_email=email)

    # create the user
    user = user_manager.create_cognito_only_user(user_id, username, full_name=full_name)
    assert user.id == user_id
    assert user.item['userId'] == user_id
    assert user.item['username'] == username
    assert user.item['fullName'] == full_name
    assert user.item['email'] == email
    assert 'phoneNumber' not in user.item

    # double check user got into db
    user = user_manager.get_user(user_id)
    assert user.id == user_id
    assert user.item['userId'] == user_id
    assert user.item['username'] == username
    assert user.item['fullName'] == full_name
    assert user.item['email'] == email
    assert 'phoneNumber' not in user.item

    # check cognito was set correctly
    assert user.cognito_client.get_user_attributes(user.id)['preferred_username'] == username


def test_create_cognito_user_aleady_exists(user_manager, cognito_client):
    user_id = 'my-user-id'
    username = 'orgusername'
    full_name = 'my-full-name'

    # create the user in the userpool (frontend does this in live system)
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')

    # create the user
    user = user_manager.create_cognito_only_user(user_id, username, full_name=full_name)
    assert user.id == user_id
    assert user.username == username

    # check their cognito username is as expected
    assert user.cognito_client.get_user_attributes(user.id)['preferred_username'] == username

    # try to create the user again, this time with a diff username
    with pytest.raises(UserAlreadyExists):
        user_manager.create_cognito_only_user(user_id, 'diffusername')

    # verify that did not affect either dynamo, cognito or pinpoint
    user = user_manager.get_user(user_id)
    assert user.username == username
    assert user.cognito_client.get_user_attributes(user.id)['preferred_username'] == username


def test_create_cognito_user_with_email_and_phone(user_manager, cognito_client):
    user_id = 'my-user-id'
    username = 'therealuser'
    full_name = 'my-full-name'
    email = 'great@best.com'
    phone = '+123'

    # frontend does this part out-of-band: creates the user in cognito with verified email and phone
    cognito_client.user_pool_client.admin_create_user(
        UserPoolId=cognito_client.user_pool_id,
        Username=user_id,
        UserAttributes=[
            {'Name': 'email', 'Value': email},
            {'Name': 'email_verified', 'Value': 'true'},
            {'Name': 'phone_number', 'Value': phone},
            {'Name': 'phone_number_verified', 'Value': 'true'},
        ],
    )

    # check the user doesn't already exist
    user = user_manager.get_user(user_id)
    assert user is None

    # create the user
    user = user_manager.create_cognito_only_user(user_id, username, full_name=full_name)
    assert user.id == user_id
    assert user.item['userId'] == user_id
    assert user.item['username'] == username
    assert user.item['fullName'] == full_name
    assert user.item['email'] == email
    assert user.item['phoneNumber'] == phone

    # check cognito attrs are as expected
    cognito_attrs = user.cognito_client.get_user_attributes(user.id)
    assert cognito_attrs['preferred_username'] == username
    assert cognito_attrs['email'] == email
    assert cognito_attrs['email_verified'] == 'true'
    assert cognito_attrs['phone_number'] == phone
    assert cognito_attrs['phone_number_verified'] == 'true'


def test_create_cognito_user_with_non_verified_email_and_phone(user_manager, cognito_client):
    user_id = 'my-user-id'
    username = 'therealuser'
    full_name = 'my-full-name'
    email = 'great@best.com'
    phone = '+123'

    # frontend does this part out-of-band: creates the user in cognito with unverified email and phone
    cognito_client.user_pool_client.admin_create_user(
        UserPoolId=cognito_client.user_pool_id,
        Username=user_id,
        UserAttributes=[
            {'Name': 'email', 'Value': email},
            {'Name': 'email_verified', 'Value': 'false'},
            {'Name': 'phone_number', 'Value': phone},
            {'Name': 'phone_number_verified', 'Value': 'false'},
        ],
    )

    # check the user doesn't already exist
    user = user_manager.get_user(user_id)
    assert user is None

    # check can't create the user
    with pytest.raises(UserValidationException):
        user_manager.create_cognito_only_user(user_id, username, full_name=full_name)


def test_create_cognito_only_user_invalid_username(user_manager):
    user_id = 'my-user-id'
    invalid_username = '-'
    full_name = 'my-full-name'

    with pytest.raises(UserValidationException):
        user_manager.create_cognito_only_user(user_id, invalid_username, full_name=full_name)


def test_create_cognito_only_user_username_taken(user_manager, cognito_only_user, cognito_client):
    user_id = 'uid'
    username_1 = cognito_only_user.username.upper()
    username_2 = cognito_only_user.username.lower()

    # frontend does this part out-of-band: creates the user in cognito, no preferred_username
    cognito_client.user_pool_client.admin_create_user(UserPoolId=cognito_client.user_pool_id, Username=user_id)

    # moto doesn't seem to honor the 'make preferred usernames unique' setting (using it as an alias)
    # so mock it's response like to simulate that it does
    exception = user_manager.cognito_client.user_pool_client.exceptions.AliasExistsException({}, None)
    user_manager.cognito_client.set_user_attributes = mock.Mock(side_effect=exception)

    with pytest.raises(UserValidationException):
        user_manager.create_cognito_only_user(user_id, username_1)

    with pytest.raises(UserValidationException):
        user_manager.create_cognito_only_user(user_id, username_2)


def test_create_cognito_only_user_username_released_if_user_not_found_in_user_pool(user_manager, cognito_client):
    # two users, one username, cognito only has a user set up for one of them
    user_id_1 = 'my-user-id-1'
    user_id_2 = 'my-user-id-2'
    username = 'myUsername'
    cognito_client.user_pool_client.admin_create_user(
        UserPoolId=cognito_client.user_pool_id,
        Username=user_id_2,
        MessageAction='SUPPRESS',
        UserAttributes=[{'Name': 'email', 'Value': 'test@real.app'}, {'Name': 'email_verified', 'Value': 'true'}],
    )

    # create the first user that doesn't exist in the user pool, should fail
    with pytest.raises(UserValidationException):
        user_manager.create_cognito_only_user(user_id_1, username)

    # should be able to now use that same username with the other user
    user = user_manager.create_cognito_only_user(user_id_2, username)
    assert user.username == username
    assert cognito_client.get_user_attributes(user.id)['preferred_username'] == username.lower()


def test_create_cognito_only_user_follow_real_user_doesnt_exist(user_manager, cognito_client):
    # create a user, verify no followeds
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    user = user_manager.create_cognito_only_user(user_id, username)
    assert list(user.follower_manager.dynamo.generate_followed_items(user.id)) == []


def test_create_cognito_only_user_follow_real_user_if_exists(user_manager, cognito_client, real_user):
    # create a user, verify follows real user
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    user = user_manager.create_cognito_only_user(user_id, username)
    followeds = list(user.follower_manager.dynamo.generate_followed_items(user.id))
    assert len(followeds) == 1
    assert followeds[0]['followedUserId'] == real_user.id
