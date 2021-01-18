import logging
import uuid

import pendulum
import pytest

from migrations.media_object_2_0_move_checksum_to_post import Migration


@pytest.fixture
def media_without_checksum(dynamo_table):
    post_id = str(uuid.uuid4())
    media_id = str(uuid.uuid4())
    media_item = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'mediaId': media_id,
        'postId': post_id,
        'postedAt': pendulum.now('utc').to_iso8601_string(),
    }
    dynamo_table.put_item(Item=media_item)
    yield media_item


@pytest.fixture
def media_without_posted_at(dynamo_table):
    post_id = str(uuid.uuid4())
    media_id = str(uuid.uuid4())
    media_item = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'mediaId': media_id,
        'postId': post_id,
        'checksum': str(uuid.uuid4()),
    }
    dynamo_table.put_item(Item=media_item)
    yield media_item


@pytest.fixture
def media_without_post_id(dynamo_table):
    media_id = str(uuid.uuid4())
    media_item = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'mediaId': media_id,
        'postedAt': pendulum.now('utc').to_iso8601_string(),
        'checksum': str(uuid.uuid4()),
    }
    dynamo_table.put_item(Item=media_item)
    yield media_item


@pytest.fixture
def media_no_post(dynamo_table):
    post_id = str(uuid.uuid4())
    media_id = str(uuid.uuid4())
    media_item = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'mediaId': media_id,
        'postId': post_id,
        'postedAt': pendulum.now('utc').to_iso8601_string(),
        'checksum': str(uuid.uuid4()),
    }
    dynamo_table.put_item(Item=media_item)
    yield media_item


@pytest.fixture
def media_post_with_checksum(dynamo_table):
    posted_at = pendulum.now('utc').to_iso8601_string()

    post_id = str(uuid.uuid4())
    post_item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'checksum': str(uuid.uuid4()),
        'postedAt': posted_at,
    }
    dynamo_table.put_item(Item=post_item)

    media_id = str(uuid.uuid4())
    media_item = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'mediaId': media_id,
        'postId': post_id,
        'postedAt': posted_at,
        'checksum': str(uuid.uuid4()),
    }
    dynamo_table.put_item(Item=media_item)
    yield media_item


@pytest.fixture
def media_post_has_diff_posted_at(dynamo_table):
    post_id = str(uuid.uuid4())
    post_item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postedAt': pendulum.now('utc').to_iso8601_string(),
    }
    dynamo_table.put_item(Item=post_item)

    media_id = str(uuid.uuid4())
    media_item = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'mediaId': media_id,
        'postId': post_id,
        'postedAt': pendulum.now('utc').to_iso8601_string(),
        'checksum': str(uuid.uuid4()),
    }
    dynamo_table.put_item(Item=media_item)
    yield media_item


@pytest.fixture
def media(dynamo_table):
    posted_at_str = pendulum.now('utc').to_iso8601_string()
    checksum = str(uuid.uuid4())

    post_id = str(uuid.uuid4())
    post_item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postedAt': posted_at_str,
    }
    dynamo_table.put_item(Item=post_item)

    media_id = str(uuid.uuid4())
    media_item = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'mediaId': media_id,
        'postId': post_id,
        'postedAt': posted_at_str,
        'checksum': checksum,
        'gsiK1PartitionKey': f'media/{checksum}',
        'gsiK1SortKey': posted_at_str,
    }
    dynamo_table.put_item(Item=media_item)
    yield media_item


media2 = media
media3 = media


def test_no_media(dynamo_client, dynamo_table, caplog):
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0


def test_media_without_checksum_skipped(dynamo_client, dynamo_table, caplog, media_without_checksum):
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0


def test_media_without_posted_at_errors(dynamo_client, dynamo_table, caplog, media_without_posted_at):
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        with pytest.raises(AssertionError):
            migration.run()
    assert len(caplog.records) == 1
    assert media_without_posted_at['mediaId'] in str(caplog.records[0])


def test_media_without_post_id_errors(dynamo_client, dynamo_table, caplog, media_without_post_id):
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        with pytest.raises(AssertionError):
            migration.run()
    assert media_without_post_id['mediaId'] in str(caplog.records[0])


def test_media_post_with_checksum_errors(dynamo_client, dynamo_table, caplog, media_post_with_checksum):
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        with pytest.raises(dynamo_client.exceptions.TransactionCanceledException):
            migration.run()
    assert media_post_with_checksum['mediaId'] in str(caplog.records[0])


def test_media_post_has_diff_posted_at_errors(dynamo_client, dynamo_table, caplog, media_post_has_diff_posted_at):
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        with pytest.raises(dynamo_client.exceptions.TransactionCanceledException):
            migration.run()
    assert media_post_has_diff_posted_at['mediaId'] in str(caplog.records[0])


def test_one_media_detailed_success(dynamo_client, dynamo_table, caplog, media):
    media_id = media['mediaId']
    post_id = media['postId']
    checksum = media['checksum']
    posted_at_str = media['postedAt']

    media_pk = {'partitionKey': f'media/{media_id}', 'sortKey': '-'}
    post_pk = {'partitionKey': f'post/{post_id}', 'sortKey': '-'}

    # verify starting state
    assert (org_media := dynamo_table.get_item(Key=media_pk)['Item'])
    assert org_media['checksum'] == checksum
    assert org_media['gsiK1PartitionKey']
    assert org_media['gsiK1SortKey']

    assert (org_post := dynamo_table.get_item(Key=post_pk)['Item'])
    assert 'checksum' not in org_post
    assert 'gsiK2PartitionKey' not in org_post
    assert 'gsiK2SortKey' not in org_post

    # run the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert media_id in str(caplog.records[0])

    # verify final state
    assert (new_media := dynamo_table.get_item(Key=media_pk)['Item'])
    assert 'checksum' not in new_media
    assert 'gsiK1PartitionKey' not in new_media
    assert 'gsiK1SortKey' not in new_media
    org_media.pop('checksum')
    org_media.pop('gsiK1PartitionKey')
    org_media.pop('gsiK1SortKey')
    assert org_media == new_media

    assert (new_post := dynamo_table.get_item(Key=post_pk)['Item'])
    assert new_post['checksum'] == checksum
    assert new_post['gsiK2PartitionKey'] == f'postChecksum/{checksum}'
    assert new_post['gsiK2SortKey'] == posted_at_str
    new_post.pop('checksum')
    new_post.pop('gsiK2PartitionKey')
    new_post.pop('gsiK2SortKey')
    assert org_post == new_post


def test_multiple_media(dynamo_client, dynamo_table, caplog, media, media2, media3):
    media1 = media

    checksum_1 = media1['checksum']
    checksum_2 = media2['checksum']
    checksum_3 = media3['checksum']

    media_id_1 = media1['mediaId']
    media_id_2 = media2['mediaId']
    media_id_3 = media3['mediaId']

    post_id_1 = media1['postId']
    post_id_2 = media2['postId']
    post_id_3 = media3['postId']

    media_pk_1 = {'partitionKey': f'media/{media_id_1}', 'sortKey': '-'}
    media_pk_2 = {'partitionKey': f'media/{media_id_2}', 'sortKey': '-'}
    media_pk_3 = {'partitionKey': f'media/{media_id_3}', 'sortKey': '-'}

    post_pk_1 = {'partitionKey': f'post/{post_id_1}', 'sortKey': '-'}
    post_pk_2 = {'partitionKey': f'post/{post_id_2}', 'sortKey': '-'}
    post_pk_3 = {'partitionKey': f'post/{post_id_3}', 'sortKey': '-'}

    # verify starting state
    assert dynamo_table.get_item(Key=media_pk_1)['Item']['checksum'] == checksum_1
    assert dynamo_table.get_item(Key=media_pk_2)['Item']['checksum'] == checksum_2
    assert dynamo_table.get_item(Key=media_pk_3)['Item']['checksum'] == checksum_3

    assert 'checksum' not in dynamo_table.get_item(Key=post_pk_1)['Item']
    assert 'checksum' not in dynamo_table.get_item(Key=post_pk_2)['Item']
    assert 'checksum' not in dynamo_table.get_item(Key=post_pk_3)['Item']

    # run the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert media_id_1 in str(caplog.records[0])
    assert media_id_2 in str(caplog.records[1])
    assert media_id_3 in str(caplog.records[2])

    # verify final state
    assert 'checksum' not in dynamo_table.get_item(Key=media_pk_1)['Item']
    assert 'checksum' not in dynamo_table.get_item(Key=media_pk_2)['Item']
    assert 'checksum' not in dynamo_table.get_item(Key=media_pk_3)['Item']

    assert dynamo_table.get_item(Key=post_pk_1)['Item']['checksum'] == checksum_1
    assert dynamo_table.get_item(Key=post_pk_2)['Item']['checksum'] == checksum_2
    assert dynamo_table.get_item(Key=post_pk_3)['Item']['checksum'] == checksum_3
