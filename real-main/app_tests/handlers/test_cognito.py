import pytest

from app.handlers.cognito import (
    CognitoClientException,
    validate_user_attribute_lowercase,
    validate_username_format,
)


def test_uuid_missing():
    with pytest.raises(CognitoClientException):
        validate_username_format({})


def test_invalid_uuid_human_readable():
    with pytest.raises(CognitoClientException):
        validate_username_format({'userName': 'bob'})


def test_invalid_uuid_hex():
    with pytest.raises(CognitoClientException):
        validate_username_format({'userName': '06d043364bda7146fa9eb6ce7763944a'})


def test_invalid_uuid_uppercase():
    with pytest.raises(CognitoClientException):
        validate_username_format({'userName': 'D4C0CC21-AAF6-4CE4-A97A-76890FB0EFBA'})


def test_invalid_uuid_too_long():
    uuid = 'us-east-1:4285bb5d-f936-4fab-a2a8-a45e534620ea'
    with pytest.raises(CognitoClientException):
        validate_username_format({'userName': f'{uuid}-extrastuff'})
    with pytest.raises(CognitoClientException):
        validate_username_format({'userName': f'extrastuff-{uuid}'})


def test_valid_uuid():
    assert validate_username_format({'userName': 'us-east-1:4285bb5d-f936-4fab-a2a8-a45e534620ea'}) is None


def test_user_attribute_missing():
    # two versions of this seen in the wild
    event = {'request': {'userAttributes': None}}
    assert validate_user_attribute_lowercase(event, 'missing') is None

    event = {'request': {'userAttributes': {'otherOne': 'meh'}}}
    assert validate_user_attribute_lowercase(event, 'missing') is None


def test_invalid_user_attribute():
    event = {'request': {'userAttributes': {'att': 'aBcD@34s'}}}
    with pytest.raises(CognitoClientException):
        validate_user_attribute_lowercase(event, 'att')


def test_valid_user_attribute():
    event = {'request': {'userAttributes': {'att': 'abcd@34s'}}}
    assert validate_user_attribute_lowercase(event, 'att') is None
