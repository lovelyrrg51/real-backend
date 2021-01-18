import logging
from decimal import Decimal
from uuid import uuid4

import pytest

from migrations.post_trending_0_1_remove_non_verified_non_original import Migration


@pytest.fixture
def text_only(dynamo_table):
    post_id = str(uuid4())
    dynamo_table.put_item(
        Item={'partitionKey': f'post/{post_id}', 'sortKey': '-', 'postId': post_id, 'postType': 'TEXT_ONLY'}
    )
    dynamo_table.put_item(
        Item={
            'partitionKey': f'post/{post_id}',
            'sortKey': 'trending',
            'gsiK3PartitionKey': 'post/trending',
            'gsiK3SortKey': Decimal(1),
        }
    )
    yield dynamo_table.get_item(Key={'partitionKey': f'post/{post_id}', 'sortKey': 'trending'})['Item']


@pytest.fixture
def image_ok_1(dynamo_table):
    post_id = str(uuid4())
    dynamo_table.put_item(
        Item={
            'partitionKey': f'post/{post_id}',
            'sortKey': '-',
            'postId': post_id,
            'postType': 'IMAGE',
            'isVerified': True,
            'originalPostId': post_id,
        }
    )
    dynamo_table.put_item(
        Item={
            'partitionKey': f'post/{post_id}',
            'sortKey': 'trending',
            'gsiK3PartitionKey': 'post/trending',
            'gsiK3SortKey': Decimal(1),
        }
    )
    yield dynamo_table.get_item(Key={'partitionKey': f'post/{post_id}', 'sortKey': 'trending'})['Item']


@pytest.fixture
def image_ok_2(dynamo_table):
    post_id = str(uuid4())
    dynamo_table.put_item(
        Item={'partitionKey': f'post/{post_id}', 'sortKey': '-', 'postId': post_id, 'isVerified': True}
    )
    dynamo_table.put_item(
        Item={
            'partitionKey': f'post/{post_id}',
            'sortKey': 'trending',
            'gsiK3PartitionKey': 'post/trending',
            'gsiK3SortKey': Decimal(1),
        }
    )
    yield dynamo_table.get_item(Key={'partitionKey': f'post/{post_id}', 'sortKey': 'trending'})['Item']


@pytest.fixture
def not_verified(dynamo_table):
    post_id = str(uuid4())
    dynamo_table.put_item(Item={'partitionKey': f'post/{post_id}', 'sortKey': '-', 'postId': post_id})
    dynamo_table.put_item(
        Item={
            'partitionKey': f'post/{post_id}',
            'sortKey': 'trending',
            'gsiK3PartitionKey': 'post/trending',
            'gsiK3SortKey': Decimal(1),
        }
    )
    yield dynamo_table.get_item(Key={'partitionKey': f'post/{post_id}', 'sortKey': 'trending'})['Item']


@pytest.fixture
def not_original(dynamo_table):
    post_id = str(uuid4())
    dynamo_table.put_item(
        Item={
            'partitionKey': f'post/{post_id}',
            'sortKey': '-',
            'postId': post_id,
            'isVerified': True,
            'originalPostId': str(uuid4()),
        }
    )
    dynamo_table.put_item(
        Item={
            'partitionKey': f'post/{post_id}',
            'sortKey': 'trending',
            'gsiK3PartitionKey': 'post/trending',
            'gsiK3SortKey': Decimal(1),
        }
    )
    yield dynamo_table.get_item(Key={'partitionKey': f'post/{post_id}', 'sortKey': 'trending'})['Item']


def test_migrate_none_to_migrate(dynamo_client, dynamo_table, caplog, text_only, image_ok_1, image_ok_2):
    # verify starting state
    trendings = [text_only, image_ok_1, image_ok_2]
    keys = [{'partitionKey': t['partitionKey'], 'sortKey': t['sortKey']} for t in trendings]
    for key, trending in zip(keys, trendings):
        assert dynamo_table.get_item(Key=key)['Item'] == trending

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert all('leaving' in rec.msg for rec in caplog.records)
    assert sum(keys[0]['partitionKey'] in rec.msg for rec in caplog.records) == 1
    assert sum(keys[1]['partitionKey'] in rec.msg for rec in caplog.records) == 1
    assert sum(keys[2]['partitionKey'] in rec.msg for rec in caplog.records) == 1

    # verify final state
    for key, trending in zip(keys, trendings):
        assert dynamo_table.get_item(Key=key)['Item'] == trending


@pytest.mark.parametrize('trending', pytest.lazy_fixture(['not_verified', 'not_original']))
def test_migrate_one(dynamo_client, dynamo_table, caplog, trending):
    # verify starting state
    key = {'partitionKey': trending['partitionKey'], 'sortKey': trending['sortKey']}
    assert dynamo_table.get_item(Key=key)['Item'] == trending

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'deleting' in caplog.records[0].msg
    assert key['partitionKey'] in caplog.records[0].msg

    # verify final state
    assert 'Item' not in dynamo_table.get_item(Key=key)


def test_migrate_all(
    dynamo_client, dynamo_table, caplog, text_only, image_ok_1, image_ok_2, not_verified, not_original
):
    # verify starting state
    ok_trendings = [text_only, image_ok_1, image_ok_2]
    bad_trendings = [not_verified, not_original]
    ok_keys = [{'partitionKey': t['partitionKey'], 'sortKey': t['sortKey']} for t in ok_trendings]
    bad_keys = [{'partitionKey': t['partitionKey'], 'sortKey': t['sortKey']} for t in bad_trendings]
    for key, trending in zip(ok_keys + bad_keys, ok_trendings + bad_trendings):
        assert dynamo_table.get_item(Key=key)['Item'] == trending

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 5
    leaving_records = [rec for rec in caplog.records if 'leaving' in rec.msg]
    deleting_records = [rec for rec in caplog.records if 'deleting' in rec.msg]
    assert len(leaving_records) == 3
    assert len(deleting_records) == 2
    assert sum(ok_keys[0]['partitionKey'] in rec.msg for rec in leaving_records) == 1
    assert sum(ok_keys[1]['partitionKey'] in rec.msg for rec in leaving_records) == 1
    assert sum(ok_keys[2]['partitionKey'] in rec.msg for rec in leaving_records) == 1
    assert sum(bad_keys[0]['partitionKey'] in rec.msg for rec in deleting_records) == 1
    assert sum(bad_keys[1]['partitionKey'] in rec.msg for rec in deleting_records) == 1

    # verify final state
    for key, trending in zip(ok_keys, ok_trendings):
        assert dynamo_table.get_item(Key=key)['Item'] == trending
    for key in bad_keys:
        assert 'Item' not in dynamo_table.get_item(Key=key)
