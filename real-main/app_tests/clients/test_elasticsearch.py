import pytest
import requests_mock

from app.clients import ElasticSearchClient

# the requests_mock parameter is auto-supplied, no need to even import the
# requests-mock library # https://requests-mock.readthedocs.io/en/latest/pytest.html


@pytest.fixture
def elasticsearch_client():
    yield ElasticSearchClient(domain='real.es.amazonaws.com')


def test_build_user_url(elasticsearch_client):
    user_id = 'my-user-id'
    assert elasticsearch_client.build_user_url(user_id) == 'https://real.es.amazonaws.com/users/_doc/my-user-id'


def test_build_user_document_minimal(elasticsearch_client):
    user_id = 'us-east-1:088d2841-7089-4136-88a0-8aa3e5ae9ce1'
    username = 'TESTER-gotSOMEcaseotxxie'
    assert elasticsearch_client.build_user_doc(user_id, username, None) == {
        'userId': 'us-east-1:088d2841-7089-4136-88a0-8aa3e5ae9ce1',
        'username': 'TESTER-gotSOMEcaseotxxie',
    }

    user_id = 'us-east-1:bca9f0ae-76e4-4ac9-a750-c691cbda505b'
    username = 'TESTER-o7jow8'
    full_name = 'Joe Shmoe'
    assert elasticsearch_client.build_user_doc(user_id, username, full_name) == {
        'userId': 'us-east-1:bca9f0ae-76e4-4ac9-a750-c691cbda505b',
        'username': 'TESTER-o7jow8',
        'fullName': 'Joe Shmoe',
    }


def test_put_user_minimal(elasticsearch_client, monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'foo')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'bar')

    user_id = 'us-east-1:088d2841-7089-4136-88a0-8aa3e5ae9ce1'
    username = 'TESTER-gotSOMEcaseotxxie'
    doc = elasticsearch_client.build_user_doc(user_id, username, None)
    url = elasticsearch_client.build_user_url(user_id)

    with requests_mock.mock() as m:
        m.put(url, None)
        elasticsearch_client.put_user(user_id, username, None)

    assert len(m.request_history) == 1
    assert m.request_history[0].method == 'PUT'
    assert m.request_history[0].json() == doc


def test_put_user_maximal(elasticsearch_client, monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'foo')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'bar')

    user_id = 'us-east-1:088d2841-7089-4136-88a0-8aa3e5ae9ce1'
    username = 'something-else'
    full_name = 'Mr. Smith'
    doc = elasticsearch_client.build_user_doc(user_id, username, full_name)
    url = elasticsearch_client.build_user_url(user_id)

    with requests_mock.mock() as m:
        m.put(url, None)
        elasticsearch_client.put_user(user_id, username, full_name)

    assert len(m.request_history) == 1
    assert m.request_history[0].method == 'PUT'
    assert m.request_history[0].json() == doc


def test_delete_user(elasticsearch_client, monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'foo')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'bar')

    user_id = 'us-east-1:088d2841-7089-4136-88a0-8aa3e5ae9ce1'
    url = elasticsearch_client.build_user_url(user_id)

    with requests_mock.mock() as m:
        m.delete(url, None)
        elasticsearch_client.delete_user(user_id)

    assert len(m.request_history) == 1
    assert m.request_history[0].method == 'DELETE'
