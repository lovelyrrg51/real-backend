import pytest

from app.clients import PostVerificationClient


@pytest.fixture
def post_verification_client():
    yield PostVerificationClient(lambda: {'root': 'https://url-root/', 'key': 'the-api-key'})


def test_verify_image_success_minimal(post_verification_client, requests_mock):
    # configure requests mock
    requests_mock.post('https://url-root/verify/image', json={'errors': [], 'data': {'isVerified': True}})

    # do the call
    result = post_verification_client.verify_image('https://image-url')
    assert result is True

    # configure requests mock
    assert len(requests_mock.request_history) == 1
    req = requests_mock.request_history[0]
    assert req.method == 'POST'
    assert req.url == 'https://url-root/verify/image'
    assert req.json() == {
        'metadata': {},
        'url': 'https://image-url',
    }
    assert req._request.headers['x-api-key'] == 'the-api-key'


def test_verify_image_success_maximal(post_verification_client, requests_mock):
    # configure requests mock
    requests_mock.post('https://url-root/verify/image', json={'errors': [], 'data': {'isVerified': False}})

    # do the call
    result = post_verification_client.verify_image('https://url', taken_in_real=True, original_format='pink')
    assert result is False

    # configure requests mock
    assert len(requests_mock.request_history) == 1
    req = requests_mock.request_history[0]
    assert req.method == 'POST'
    assert req.url == 'https://url-root/verify/image'
    assert req.json() == {
        'metadata': {'takenInReal': True, 'originalFormat': 'pink'},
        'url': 'https://url',
    }
    assert req._request.headers['x-api-key'] == 'the-api-key'


def test_verify_image_handle_400_error(post_verification_client, requests_mock):
    # configure requests mock
    error_msg = 'Your request was messed up'
    requests_mock.post('https://url-root/verify/image', json={'errors': [error_msg], 'data': {}})

    # do the call
    with pytest.raises(Exception, match=error_msg):
        post_verification_client.verify_image('https://image-url')


def test_verify_image_handle_bad_resp_fmt(post_verification_client, requests_mock):
    # configure requests mock
    requests_mock.post('https://url-root/verify/image', json={'errors': []})

    # do the call
    with pytest.raises(Exception, match='Unable to parse response'):
        post_verification_client.verify_image('https://image-url')
