import string

import pytest

from app.models.user.exceptions import UserValidationException


def test_empty_username_fails_validation(user_manager):
    with pytest.raises(UserValidationException):
        user_manager.validate_username(None)

    with pytest.raises(UserValidationException):
        user_manager.validate_username('')


def test_username_length_fails_validation(user_manager):
    with pytest.raises(UserValidationException):
        user_manager.validate_username('a' * 31)

    with pytest.raises(UserValidationException):
        user_manager.validate_username('aa')


def test_username_bad_chars_fails_validation(user_manager):
    bad_chars = set(string.printable) - set(string.digits + string.ascii_letters + '_.')
    for bad_char in bad_chars:
        with pytest.raises(UserValidationException):
            user_manager.validate_username('aaa' + bad_char)
        with pytest.raises(UserValidationException):
            user_manager.validate_username(bad_char + 'aaa')
        with pytest.raises(UserValidationException):
            user_manager.validate_username(bad_char * 3)


def test_good_username_validates(user_manager):
    user_manager.validate_username('buzz_lightyear')
    user_manager.validate_username('buzz.lightyear')
    user_manager.validate_username('UpAndOver')
    user_manager.validate_username('__.0009A_...')
