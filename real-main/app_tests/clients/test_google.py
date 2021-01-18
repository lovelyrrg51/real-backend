from unittest import mock

import pytest

from app.clients import GoogleClient

# this an actual real response, if it's not obvious
google_id_info = {
    "iss": "https://accounts.google.com",
    "azp": "web-google-client-id",
    "aud": "web-google-client-id",
    "sub": "101274498972592384425",
    "hd": "real.app",
    "email": "mike@real.app",
    "email_verified": "true",
    "at_hash": "vHojrhEDEK1fPNhEEm21mg",
    "name": "Mike Fogel",
    "picture": "https://lh3.googleusercontent.com/truncated.jpg",
    "given_name": "Mike",
    "family_name": "Fogel",
    "locale": "en",
    "iat": "1574788742",
    "exp": "1574792342",
    "alg": "RS256",
    "kid": "dee8d3dafbf31262ab9347d620383217afd96ca3",
    "typ": "JWT",
}


def client_ids_getter():
    return {
        'ios': 'ios-google-client-id',
        'web': 'web-google-client-id',
    }


@pytest.mark.parametrize("id_token", [None, '----'])
def test_token_badly_invalid(id_token):
    google_client = GoogleClient(client_ids_getter)
    with pytest.raises(ValueError):
        google_client.get_verified_email(id_token)


def test_token_wrong_audience():
    with mock.patch('app.clients.google.google_id_token') as google_id_token:
        google_id_token.verify_oauth2_token.return_value = {**google_id_info, **{'aud': 'anything else'}}
        google_client = GoogleClient(client_ids_getter)
        with pytest.raises(ValueError, match='audience'):
            google_client.get_verified_email(None)


def test_token_email_not_verified():
    with mock.patch('app.clients.google.google_id_token') as google_id_token:
        google_id_token.verify_oauth2_token.return_value = {
            k: v for k, v in google_id_info.items() if k != 'email_verified'
        }
        google_client = GoogleClient(client_ids_getter)
        with pytest.raises(ValueError, match='verified email'):
            google_client.get_verified_email(None)


def test_token_no_email():
    with mock.patch('app.clients.google.google_id_token') as google_id_token:
        google_id_token.verify_oauth2_token.return_value = {
            k: v for k, v in google_id_info.items() if k != 'email'
        }
        google_client = GoogleClient(client_ids_getter)
        with pytest.raises(ValueError, match='verified email'):
            google_client.get_verified_email(None)


def test_token_valid():
    # the token actually is expired, but the timestamp check is behind the mock so it's skipped
    with mock.patch('app.clients.google.google_id_token') as google_id_token:
        google_id_token.verify_oauth2_token.return_value = google_id_info
        google_client = GoogleClient(client_ids_getter)
        email = google_client.get_verified_email(None)
        assert email == 'mike@real.app'
