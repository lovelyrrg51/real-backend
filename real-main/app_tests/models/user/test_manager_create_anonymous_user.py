from unittest import mock
from uuid import uuid4

import pytest

from app.models.user.enums import UserStatus
from app.models.user.exceptions import UserValidationException


@pytest.fixture
def real_user(user_manager, cognito_client):
    user_id = str(uuid4())
    cognito_client.create_user_pool_entry(user_id, 'real', verified_email='real-test@real.app')
    yield user_manager.create_cognito_only_user(user_id, 'real')


def test_create_anonymous_user_success(user_manager, real_user):
    cognito_id_token = 'cog-token'
    cognito_tokens = {'IdToken': cognito_id_token}
    user_id = str(uuid4())

    # set up our mocks to behave correctly
    user_manager.cognito_client.create_user_pool_entry = mock.Mock()
    user_manager.cognito_client.get_user_pool_tokens = mock.Mock(return_value=cognito_tokens)
    user_manager.cognito_client.link_identity_pool_entries = mock.Mock()

    # create the user, check it is as expected
    user, tokens = user_manager.create_anonymous_user(user_id)
    assert tokens == cognito_tokens
    assert user.id == user_id
    username = user.item['username']
    user_manager.validate_username(username)  # throws exception on failure
    assert 'fullName' not in user.item
    assert 'email' not in user.item
    assert 'phoneNumber' not in user.item
    assert user.status == UserStatus.ANONYMOUS

    # check mocks called as expected
    assert user_manager.cognito_client.create_user_pool_entry.mock_calls == [mock.call(user_id, username)]
    assert user_manager.cognito_client.get_user_pool_tokens.mock_calls == [mock.call(user_id)]
    assert user_manager.cognito_client.link_identity_pool_entries.mock_calls == [
        mock.call(
            user_id,
            cognito_token=cognito_id_token,
        ),
    ]

    # check we are following the real user
    followeds = list(user.follower_manager.dynamo.generate_followed_items(user.id))
    assert len(followeds) == 1
    assert followeds[0]['followedUserId'] == real_user.id


def test_create_anonymous_user_user_id_taken(user_manager):
    # configure cognito to respond as if user_id is already taken
    user_id = str(uuid4())
    exception = user_manager.cognito_client.user_pool_client.exceptions.UsernameExistsException(
        {'Error': {'Code': '<code>', 'Message': 'User account already exists.'}}, '<operation name>'
    )
    user_manager.cognito_client.user_pool_client.admin_create_user = mock.Mock(side_effect=exception)
    with pytest.raises(UserValidationException, match=f'An account for userId `{user_id}` already exists'):
        user_manager.create_anonymous_user(user_id)


def test_create_anonymous_user_cognito_identity_pool_exception_cleansup(user_manager):
    user_id = str(uuid4())

    # set up our mocks to behave correctly
    user_manager.cognito_client.create_user_pool_entry = mock.Mock()
    user_manager.cognito_client.get_user_pool_tokens = mock.Mock(return_value={'IdToken': 'cog-token'})
    user_manager.cognito_client.link_identity_pool_entries = mock.Mock(side_effect=Exception('anything bad'))
    user_manager.cognito_client.delete_user_pool_entry = mock.Mock()

    # create the user, check we tried to clean up after the failure
    with pytest.raises(Exception, match='anything bad'):
        user_manager.create_anonymous_user(user_id)
    assert user_manager.cognito_client.delete_user_pool_entry.mock_calls == [mock.call(user_id)]
