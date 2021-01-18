from unittest.mock import call
from uuid import uuid4

import pytest


@pytest.mark.skip(reason='moto appears to not support CUSTOM_AUTH AuthFlow for admin_initiate_auth')
def test_get_user_pool_tokens(cognito_client):
    pass


@pytest.mark.skip(reason='moto does not support admin_delete_user_attributes yet')
def test_clear_user_attribute(cognito_client):
    pass


@pytest.mark.skip(reason='moto appears to have no implemented filters on list_users yet')
def test_list_unconfirmed_user_pool_entries(cognito_client):
    pass


def test_create_user_pool_entry_no_verified_email(cognito_client):
    user_id, username = str(uuid4()), str(uuid4())

    # check they aren't there
    with pytest.raises(cognito_client.user_pool_client.exceptions.UserNotFoundException):
        cognito_client.get_user_attributes(user_id)

    # create them, check they are there
    cognito_client.create_user_pool_entry(user_id, username)
    attrs = cognito_client.get_user_attributes(user_id)
    assert len(attrs) == 1
    assert attrs['preferred_username'] == username

    # verify we can't create them again, even with a diff username
    with pytest.raises(cognito_client.user_pool_client.exceptions.UsernameExistsException):
        cognito_client.create_user_pool_entry(user_id, username)


def test_create_user_pool_entry_with_verified_email(cognito_client):
    user_id, username = str(uuid4()), str(uuid4())
    email = f'{username}-test@real.app'

    # check they aren't there
    with pytest.raises(cognito_client.user_pool_client.exceptions.UserNotFoundException):
        cognito_client.get_user_attributes(user_id)

    # create them, check they are there
    cognito_client.create_user_pool_entry(user_id, username, verified_email=email)
    attrs = cognito_client.get_user_attributes(user_id)
    assert len(attrs) == 3
    assert attrs['email'] == email
    assert attrs['email_verified'] == 'true'
    assert attrs['preferred_username'] == username
    password = cognito_client.user_pool_client.admin_set_user_password.call_args.kwargs['Password']
    assert cognito_client.user_pool_client.admin_set_user_password.mock_calls == [
        call(UserPoolId=cognito_client.user_pool_id, Username=user_id, Password=password, Permanent=True)
    ]

    # verify we can't create them again
    with pytest.raises(cognito_client.user_pool_client.exceptions.UsernameExistsException):
        cognito_client.create_user_pool_entry(user_id, username, verified_email=email)


def test_set_and_get_user_attributes(cognito_client):
    # create an entry in the user pool, check attributes
    user_id = 'uid'
    cognito_client.user_pool_client.admin_create_user(
        UserPoolId=cognito_client.user_pool_id,
        Username=user_id,
    )
    assert cognito_client.get_user_attributes(user_id) == {}

    # set some user attributes, check
    cognito_client.set_user_attributes(user_id, {'nickname': 'sparky', 'gender': 'other'})
    attrs = cognito_client.get_user_attributes(user_id)
    assert len(attrs) == 2
    assert attrs['nickname'] == 'sparky'
    assert attrs['gender'] == 'other'

    # update existing attribute, check
    cognito_client.set_user_attributes(user_id, {'gender': 'female'})
    attrs = cognito_client.get_user_attributes(user_id)
    assert len(attrs) == 2
    assert attrs['nickname'] == 'sparky'
    assert attrs['gender'] == 'female'

    # try to set & get user attributes for a user that doesn't exist
    with pytest.raises(cognito_client.user_pool_client.exceptions.UserNotFoundException):
        cognito_client.set_user_attributes('uiddne', {'gender': 'female'})
    with pytest.raises(cognito_client.user_pool_client.exceptions.UserNotFoundException):
        cognito_client.get_user_attributes('uiddne')


def test_get_user_by_status(cognito_client):
    # create an entry in the user pool, check status
    user_id = 'uid'
    cognito_client.user_pool_client.admin_create_user(
        UserPoolId=cognito_client.user_pool_id,
        Username=user_id,
    )
    assert cognito_client.get_user_status(user_id) == 'FORCE_CHANGE_PASSWORD'

    with pytest.raises(cognito_client.user_pool_client.exceptions.UserNotFoundException):
        cognito_client.get_user_status('uiddne')


def test_delete_user_pool_entry(cognito_client):
    # cant delete a user that doesn't exist
    with pytest.raises(cognito_client.user_pool_client.exceptions.UserNotFoundException):
        cognito_client.delete_user_pool_entry('uiddne')

    # create a dummy profile, verify exists
    user_id = 'uid'
    cognito_client.user_pool_client.admin_create_user(
        UserPoolId=cognito_client.user_pool_id,
        Username=user_id,
    )
    assert cognito_client.get_user_status(user_id)

    # delete the dummy profile, verify it's gone
    cognito_client.delete_user_pool_entry(user_id)
    with pytest.raises(cognito_client.user_pool_client.exceptions.UserNotFoundException):
        cognito_client.get_user_status(user_id)
