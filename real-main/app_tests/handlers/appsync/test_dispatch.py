import os

import pytest

# turning off route autodiscovery
os.environ['APPSYNC_ROUTE_AUTODISCOVERY_PATH'] = ''
from app.handlers.appsync import dispatch, routes  # noqa: E402 isort:skip


@pytest.fixture
def cognito_authed_event():
    yield {
        'info': {
            'parentTypeName': 'Type',
            'fieldName': 'field',
        },
        'arguments': ['arg1', 'arg2'],
        'identity': {'cognitoIdentityId': '42-42'},
        'source': {'anotherField': 42},
        'request': {'headers': {'x-real-version': '1.2.3(456)'}},
    }


@pytest.fixture
def api_key_authed_event():
    yield {
        'info': {
            'parentTypeName': 'Type',
            'fieldName': 'field',
        },
        'arguments': ['arg1', 'arg2'],
        'identity': {},
        'source': {'anotherField': 42},
        'request': {'headers': {}},
    }


@pytest.fixture
def setup_one_route():
    routes.clear()

    @routes.register('Type.field')
    def mocked_handler(caller_user_id, arguments, **kwargs):  # pylint: disable=unused-variable
        return {'caller_user_id': caller_user_id, 'arguments': arguments, 'kwargs': kwargs}


def test_unknown_field_raises_exception(setup_one_route, cognito_authed_event):
    cognito_authed_event['info']['fieldName'] = 'unknownField'
    with pytest.raises(Exception, match='No handler for field `Type.unknownField` found'):
        dispatch(cognito_authed_event, {})


def test_basic_success(setup_one_route, cognito_authed_event):
    assert dispatch(cognito_authed_event, {}) == {
        'data': {
            'caller_user_id': '42-42',
            'arguments': ['arg1', 'arg2'],
            'kwargs': {
                'source': {'anotherField': 42},
                'context': {},
                'client': {'version': '1.2.3(456)'},
                'event': cognito_authed_event,
            },
        },
    }


def test_no_source(setup_one_route, cognito_authed_event):
    del cognito_authed_event['source']
    assert dispatch(cognito_authed_event, {}) == {
        'data': {
            'caller_user_id': '42-42',
            'arguments': ['arg1', 'arg2'],
            'kwargs': {
                'source': {},
                'context': {},
                'client': {'version': '1.2.3(456)'},
                'event': cognito_authed_event,
            },
        },
    }


def test_api_key_authenticated(setup_one_route, api_key_authed_event):
    assert dispatch(api_key_authed_event, {}) == {
        'data': {
            'caller_user_id': None,
            'arguments': ['arg1', 'arg2'],
            'kwargs': {
                'source': {'anotherField': 42},
                'context': {},
                'client': {},
                'event': api_key_authed_event,
            },
        },
    }


def test_context_passed(setup_one_route, api_key_authed_event):
    assert dispatch(api_key_authed_event, {'foo': 'bar'}) == {
        'data': {
            'caller_user_id': None,
            'arguments': ['arg1', 'arg2'],
            'kwargs': {
                'source': {'anotherField': 42},
                'context': {'foo': 'bar'},
                'client': {},
                'event': api_key_authed_event,
            },
        },
    }
