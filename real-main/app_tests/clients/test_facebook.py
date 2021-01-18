import json

from app.clients import FacebookClient

# the requests_mock parameter is auto-supplied, no need to even import the
# requests-mock library # https://requests-mock.readthedocs.io/en/latest/pytest.html


def test_get_verified_email_success(requests_mock):
    api_root = 'https://graph.facebook.com'
    client = FacebookClient(api_root)

    access_token = 'my-access-token'
    complete_url = f'{api_root}/me?fields=email&access_token={access_token}'
    mocked_contact_info = {"email": "mike@real.app"}

    requests_mock.get(complete_url, text=json.dumps(mocked_contact_info))
    email = client.get_verified_email(access_token)
    assert email == 'mike@real.app'
