"A test library for pinpoint designed to be run against a live Pinpoint Application"

import os
import uuid
from unittest.mock import Mock, call

import dotenv
import pytest

from app.clients import PinpointClient


@pytest.fixture
def real_pinpoint_client():
    dotenv.load_dotenv()
    app_id = os.environ.get('PINPOINT_APPLICATION_ID')
    yield PinpointClient(app_id=app_id)


@pytest.fixture
def mocked_pinpoint_client():
    pinpoint_client = PinpointClient(app_id='testing-pinpoint-app-id')
    pinpoint_client.client = Mock(pinpoint_client.client)
    yield pinpoint_client


def test_send_user_apns(mocked_pinpoint_client):
    user_id = str(uuid.uuid4())
    sample_success_resp = {
        'ResponseMetadata': {},
        'SendUsersMessageResponse': {
            'ApplicationId': '',
            'RequestId': '',
            'Result': {
                user_id: {
                    str(uuid.uuid4()): {
                        'Address': 'email-addr@real.app',
                        'DeliveryStatus': 'PERMANENT_FAILURE',
                        'StatusCode': 400,
                        'StatusMessage': 'Either template or email message must be specified',
                    },
                    str(uuid.uuid4()): {
                        'Address': '123456778890abccdef123456778890aaaaaaaaaaaaaabbbbbbbbbbbcccccccc',
                        'DeliveryStatus': 'SUCCESSFUL',
                        'StatusCode': 200,
                        'StatusMessage': '',
                    },
                }
            },
        },
    }
    sample_failure_resp = {
        'ResponseMetadata': {},
        'SendUsersMessageResponse': {
            'ApplicationId': '',
            'RequestId': '',
            'Result': {
                user_id: {
                    str(uuid.uuid4()): {
                        'Address': '123456778890abccdef123456778890aaaaaaaaaaaaaabbbbbbbbbbbcccccccc',
                        'DeliveryStatus': 'PERMANENT_FAILURE',
                        'StatusCode': 400,
                        'StatusMessage': 'No endpoints found for userId.',
                    },
                }
            },
        },
    }

    # test a success
    mocked_pinpoint_client.client.reset_mock()
    mocked_pinpoint_client.client.configure_mock(**{'send_users_messages.return_value': sample_success_resp})
    resp = mocked_pinpoint_client.send_user_apns(user_id, 'the-url', 'the-title', 'the-body')
    assert resp is True
    assert mocked_pinpoint_client.client.mock_calls == [
        call.send_users_messages(
            ApplicationId='testing-pinpoint-app-id',
            SendUsersMessageRequest={
                'MessageConfiguration': {
                    'APNSMessage': {'Action': 'URL', 'Body': 'the-body', 'Title': 'the-title', 'Url': 'the-url'}
                },
                'Users': {user_id: {}},
            },
        )
    ]

    # test a failure due to missing APNS token
    mocked_pinpoint_client.client.reset_mock()
    mocked_pinpoint_client.client.configure_mock(**{'send_users_messages.return_value': sample_failure_resp})
    resp = mocked_pinpoint_client.send_user_apns(user_id, 'the-url', 'the-title', 'the-body')
    assert resp is False
    assert mocked_pinpoint_client.client.mock_calls == [
        call.send_users_messages(
            ApplicationId='testing-pinpoint-app-id',
            SendUsersMessageRequest={
                'MessageConfiguration': {
                    'APNSMessage': {'Action': 'URL', 'Body': 'the-body', 'Title': 'the-title', 'Url': 'the-url'}
                },
                'Users': {user_id: {}},
            },
        )
    ]


@pytest.mark.skip(reason='Requires live Pinpoint Application')
@pytest.mark.parametrize(
    'channel_type, address1, address2',
    (
        ('EMAIL', 'pinpoint-test@real.app', 'pinpoint-test2@real.app'),
        ('SMS', '+14155551212', '+12125551212'),
        ('APNS', 'apns-token-1', 'apns-token-2'),
    ),
)
def test_update_and_delete_user_endpoint(real_pinpoint_client, channel_type, address1, address2):
    pinpoint_client = real_pinpoint_client
    user_id = str(uuid.uuid4())
    assert pinpoint_client.get_user_endpoints(user_id, channel_type=channel_type) == {}

    # create an endpoint, verify it exists
    endpoint_id1 = pinpoint_client.update_user_endpoint(user_id, channel_type, address1)
    user_endpoints = pinpoint_client.get_user_endpoints(user_id, channel_type=channel_type)
    assert len(user_endpoints) == 1
    assert user_endpoints[endpoint_id1]['Address'] == address1

    # no-op update the endpoint
    assert pinpoint_client.update_user_endpoint(user_id, channel_type, address1) == endpoint_id1
    assert pinpoint_client.get_user_endpoints(user_id, channel_type=channel_type) == user_endpoints

    # update the endpoint, to a new address
    assert pinpoint_client.update_user_endpoint(user_id, channel_type, address2) == endpoint_id1
    user_endpoints = pinpoint_client.get_user_endpoints(user_id, channel_type=channel_type)
    assert len(user_endpoints) == 1
    assert user_endpoints[endpoint_id1]['Address'] == address2

    # sneak behind our client's back and create a second endpoint of same type
    endpoint_id2 = str(uuid.uuid4())
    kwargs = {
        'ApplicationId': pinpoint_client.app_id,
        'EndpointId': endpoint_id2,
        'EndpointRequest': {'Address': address1, 'ChannelType': channel_type, 'User': {'UserId': user_id}},
    }
    pinpoint_client.client.update_endpoint(**kwargs)
    user_endpoints = pinpoint_client.get_user_endpoints(user_id, channel_type=channel_type)
    assert len(user_endpoints) == 2
    assert user_endpoints[endpoint_id1]['Address'] == address2
    assert user_endpoints[endpoint_id2]['Address'] == address1

    # update the endpoint again, verify that the extra endpoint is deleted as clean-up
    assert pinpoint_client.update_user_endpoint(user_id, channel_type, address1) == endpoint_id2
    user_endpoints = pinpoint_client.get_user_endpoints(user_id, channel_type=channel_type)
    assert len(user_endpoints) == 1
    assert user_endpoints[endpoint_id2]['Address'] == address1

    # delete all endpoints of that type
    pinpoint_client.delete_user_endpoint(user_id, channel_type)
    assert pinpoint_client.get_user_endpoints(user_id, channel_type=channel_type) == {}


@pytest.mark.skip(reason='Requires live Pinpoint Application')
def test_enable_disable_and_delete_user_endpoints(real_pinpoint_client):
    pinpoint_client = real_pinpoint_client
    user_id = str(uuid.uuid4())
    email = str(uuid.uuid4())[:8] + '-test@real.app'
    phone = '+14155551212'

    # create an sms endpoint and an email enpoint for the user, verify they exist
    endpoint_id1 = pinpoint_client.update_user_endpoint(user_id, 'EMAIL', email)
    endpoint_id2 = pinpoint_client.update_user_endpoint(user_id, 'SMS', phone)
    user_endpoints = pinpoint_client.get_user_endpoints(user_id)
    assert len(user_endpoints) == 2
    assert user_endpoints[endpoint_id1]['Address'] == email
    assert user_endpoints[endpoint_id2]['Address'] == phone
    assert user_endpoints[endpoint_id1]['EndpointStatus'] == 'ACTIVE'
    assert user_endpoints[endpoint_id2]['EndpointStatus'] == 'ACTIVE'

    # disable them, verify
    pinpoint_client.disable_user_endpoints(user_id)
    user_endpoints = pinpoint_client.get_user_endpoints(user_id)
    assert len(user_endpoints) == 2
    assert user_endpoints[endpoint_id1]['Address'] == email
    assert user_endpoints[endpoint_id2]['Address'] == phone
    assert user_endpoints[endpoint_id1]['EndpointStatus'] == 'INACTIVE'
    assert user_endpoints[endpoint_id2]['EndpointStatus'] == 'INACTIVE'

    # enable them, verify
    pinpoint_client.enable_user_endpoints(user_id)
    user_endpoints = pinpoint_client.get_user_endpoints(user_id)
    assert len(user_endpoints) == 2
    assert user_endpoints[endpoint_id1]['Address'] == email
    assert user_endpoints[endpoint_id2]['Address'] == phone
    assert user_endpoints[endpoint_id1]['EndpointStatus'] == 'ACTIVE'
    assert user_endpoints[endpoint_id2]['EndpointStatus'] == 'ACTIVE'

    # delete them, verify
    pinpoint_client.delete_user_endpoints(user_id)
    assert pinpoint_client.get_user_endpoints(user_id) == {}
