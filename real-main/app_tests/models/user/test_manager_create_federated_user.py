import logging
from unittest import mock
from uuid import uuid4

import pytest

from app.models.user.exceptions import UserValidationException


@pytest.fixture
def real_user(user_manager, cognito_client):
    user_id = str(uuid4())
    cognito_client.create_user_pool_entry(user_id, 'real', verified_email='real-test@real.app')
    yield user_manager.create_cognito_only_user(user_id, 'real')


@pytest.mark.parametrize('provider', ['apple', 'facebook', 'google'])
def test_create_federated_user_success(user_manager, real_user, provider):
    provider_token = 'fb-google-or-apple-token'
    cognito_token = 'cog-token'
    user_id = 'my-user-id'
    username = 'my_username'
    full_name = 'my-full-name'
    email = 'My@email.com'

    # set up our mocks to behave correctly
    user_manager.clients[provider].configure_mock(**{'get_verified_email.return_value': email})
    user_manager.cognito_client.create_user_pool_entry = mock.Mock()
    user_manager.cognito_client.get_user_pool_tokens = mock.Mock(return_value={'IdToken': cognito_token})
    user_manager.cognito_client.link_identity_pool_entries = mock.Mock()

    # create the user, check it is as expected
    user = user_manager.create_federated_user(provider, user_id, username, provider_token, full_name=full_name)
    assert user.id == user_id
    assert user.item['username'] == username
    assert user.item['fullName'] == full_name
    assert user.item['email'] == email.lower()

    # check mocks called as expected
    assert user_manager.clients[provider].mock_calls == [mock.call.get_verified_email(provider_token)]
    assert user_manager.cognito_client.create_user_pool_entry.mock_calls == [
        mock.call(user_id, username, verified_email=email.lower()),
    ]
    assert user_manager.cognito_client.get_user_pool_tokens.mock_calls == [mock.call(user_id)]
    call_kwargs = {
        'cognito_token': cognito_token,
        provider + '_token': provider_token,
    }
    assert user_manager.cognito_client.link_identity_pool_entries.mock_calls == [
        mock.call(user_id, **call_kwargs)
    ]

    # check we are following the real user
    followeds = list(user.follower_manager.dynamo.generate_followed_items(user.id))
    assert len(followeds) == 1
    assert followeds[0]['followedUserId'] == real_user.id


@pytest.mark.parametrize('provider', ['apple', 'facebook', 'google'])
def test_create_federated_user_user_id_taken(user_manager, provider):
    # configure cognito to respond as if user_id is already taken
    user_id, username = str(uuid4()), str(uuid4())[:8]
    exception = user_manager.cognito_client.user_pool_client.exceptions.UsernameExistsException(
        {'Error': {'Code': '<code>', 'Message': 'User account already exists.'}}, '<operation name>'
    )
    user_manager.cognito_client.user_pool_client.admin_create_user = mock.Mock(side_effect=exception)
    with pytest.raises(UserValidationException, match=f'An account for userId `{user_id}` already exists'):
        user_manager.create_federated_user(provider, user_id, username, 'provider-token')


@pytest.mark.parametrize('provider', ['apple', 'facebook', 'google'])
def test_create_federated_user_username_taken(user_manager, provider):
    # configure cognito to respond as if username is already taken
    user_id, username = str(uuid4()), str(uuid4())[:8]
    exception = user_manager.cognito_client.user_pool_client.exceptions.UsernameExistsException(
        {'Error': {'Code': '<code>', 'Message': 'Already found an entry for the provided username.'}},
        '<operation name>',
    )
    user_manager.cognito_client.user_pool_client.admin_create_user = mock.Mock(side_effect=exception)
    with pytest.raises(UserValidationException, match=f'Username `{username}` already taken'):
        user_manager.create_federated_user(provider, user_id, username, 'provider-token')


@pytest.mark.parametrize('provider', ['apple', 'facebook', 'google'])
def test_create_federated_user_email_taken(user_manager, provider):
    # configure cognito to respond as if email is already taken
    user_id, username = str(uuid4()), str(uuid4())[:8]
    email = f'{username}@somedomain.com'
    user_manager.clients[provider].configure_mock(**{'get_verified_email.return_value': email})
    exception = user_manager.cognito_client.user_pool_client.exceptions.UsernameExistsException(
        {'Error': {'Code': '<code>', 'Message': 'An account with the email already exists.'}},
        '<operation name>',
    )
    user_manager.cognito_client.user_pool_client.admin_create_user = mock.Mock(side_effect=exception)
    with pytest.raises(UserValidationException, match=f'Email `{email}` already taken'):
        user_manager.create_federated_user(provider, user_id, username, 'provider-token')


@pytest.mark.parametrize('provider', ['apple', 'facebook', 'google'])
def test_create_federated_user_invalid_token(user_manager, caplog, provider):
    provider_token = 'google-token'
    user_id = 'my-user-id'
    username = 'newuser'

    # set up our mocks to behave correctly
    user_manager.clients[provider].configure_mock(
        **{'get_verified_email.side_effect': ValueError('wrong flavor')}
    )

    # create the google user, check it is as expected
    with caplog.at_level(logging.WARNING):
        with pytest.raises(UserValidationException, match='wrong flavor'):
            user_manager.create_federated_user(provider, user_id, username, provider_token)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert 'wrong flavor' in caplog.records[0].msg


@pytest.mark.parametrize('provider', ['apple', 'facebook', 'google'])
def test_create_federated_user_cognito_identity_pool_exception_cleansup(user_manager, provider):
    user_id = 'my-user-id'

    # set up our mocks to behave correctly
    user_manager.clients[provider].configure_mock(**{'get_verified_email.return_value': 'me@email.com'})
    user_manager.cognito_client.create_user_pool_entry = mock.Mock()
    user_manager.cognito_client.get_user_pool_tokens = mock.Mock(return_value={'IdToken': 'cog-token'})
    user_manager.cognito_client.link_identity_pool_entries = mock.Mock(side_effect=Exception('anything bad'))
    user_manager.cognito_client.delete_user_pool_entry = mock.Mock()

    # create the user, check we tried to clean up after the failure
    with pytest.raises(Exception, match='anything bad'):
        user_manager.create_federated_user(provider, user_id, 'username', 'provider-token')
    assert user_manager.cognito_client.delete_user_pool_entry.mock_calls == [mock.call(user_id)]
