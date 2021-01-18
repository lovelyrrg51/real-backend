import logging
import uuid

import pytest

from migrations.media_object_2_1_drop_media_status import Migration


@pytest.fixture
def media_without_status(dynamo_table):
    post_id = str(uuid.uuid4())
    media_id = str(uuid.uuid4())
    media_item = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'mediaId': media_id,
        'postId': post_id,
        'gsiA1PartitionKey': f'media/{post_id}',
        'gsiA1SortKey': '-',
    }
    dynamo_table.put_item(Item=media_item)
    yield media_item


@pytest.fixture
def media(dynamo_table):
    post_id = str(uuid.uuid4())
    media_id = str(uuid.uuid4())
    media_status = media_id[:8]
    media_item = {
        'partitionKey': f'media/{media_id}',
        'sortKey': '-',
        'mediaId': media_id,
        'postId': post_id,
        'mediaStatus': media_status,
        'gsiA1PartitionKey': f'media/{post_id}',
        'gsiA1SortKey': media_status,
        'gsiA2PartitionKey': 'anything',
        'gsiA2SortKey': 'anything at all',
    }
    dynamo_table.put_item(Item=media_item)
    yield media_item


media2 = media
media3 = media


def test_nothing_to_migrate(dynamo_table, caplog, media_without_status):
    media_pk = {k: media_without_status[k] for k in ('partitionKey', 'sortKey')}

    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    assert dynamo_table.get_item(Key=media_pk)['Item'] == media_without_status


def test_one_media_detailed_success(dynamo_table, caplog, media):
    media_id = media['mediaId']
    media_pk = {k: media[k] for k in ('partitionKey', 'sortKey')}

    # verify starting state
    assert (org_media := dynamo_table.get_item(Key=media_pk)['Item'])
    assert 'mediaStatus' in org_media
    assert 'gsiA2PartitionKey' in org_media
    assert 'gsiA2SortKey' in org_media
    assert org_media['gsiA1SortKey'] != '-'

    # run the migration
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert media_id in str(caplog.records[0])

    # verify final state
    assert (new_media := dynamo_table.get_item(Key=media_pk)['Item'])
    assert 'mediaStatus' not in new_media
    assert 'gsiA2PartitionKey' not in new_media
    assert 'gsiA2SortKey' not in new_media
    assert new_media['gsiA1SortKey'] == '-'

    # make sure there are no other differences
    new_media['mediaStatus'] = org_media['mediaStatus']
    new_media['gsiA1SortKey'] = org_media['gsiA1SortKey']
    new_media['gsiA2PartitionKey'] = org_media['gsiA2PartitionKey']
    new_media['gsiA2SortKey'] = org_media['gsiA2SortKey']
    assert new_media == org_media


def test_multiple_media(dynamo_table, caplog, media, media2, media3):
    media1 = media

    media_id_1 = media1['mediaId']
    media_id_2 = media2['mediaId']
    media_id_3 = media3['mediaId']

    media_pk_1 = {'partitionKey': f'media/{media_id_1}', 'sortKey': '-'}
    media_pk_2 = {'partitionKey': f'media/{media_id_2}', 'sortKey': '-'}
    media_pk_3 = {'partitionKey': f'media/{media_id_3}', 'sortKey': '-'}

    # verify starting state
    assert 'mediaStatus' in dynamo_table.get_item(Key=media_pk_1)['Item']
    assert 'mediaStatus' in dynamo_table.get_item(Key=media_pk_2)['Item']
    assert 'mediaStatus' in dynamo_table.get_item(Key=media_pk_3)['Item']

    # run the migration
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert media_id_1 in str(caplog.records[0])
    assert media_id_2 in str(caplog.records[1])
    assert media_id_3 in str(caplog.records[2])

    # verify final state
    assert 'mediaStatus' not in dynamo_table.get_item(Key=media_pk_1)['Item']
    assert 'mediaStatus' not in dynamo_table.get_item(Key=media_pk_2)['Item']
    assert 'mediaStatus' not in dynamo_table.get_item(Key=media_pk_3)['Item']
