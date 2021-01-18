import logging
from decimal import Decimal
from random import random
from uuid import uuid4

import pendulum
import pytest

from migrations.post_trending_0_0_fill import Migration


@pytest.fixture
def not_completed_post(dynamo_table):
    post_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
        'postStatus': 'anything-but-completed',
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post1(dynamo_table):
    post_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
        'postStatus': 'COMPLETED',
    }
    dynamo_table.put_item(Item=item)
    yield item


post2 = post1
post3 = post1


@pytest.fixture
def trending1(dynamo_table, post1):
    now_str = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': post1['partitionKey'],
        'sortKey': 'trending',
        'schemaVersion': 0,
        'gsiK3PartitionKey': 'post/trending',
        'gsiK3SortKey': Decimal(random()).normalize(),
        'lastDeflatedAt': now_str,
        'createdAt': now_str,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def trending2(dynamo_table, post2):
    now_str = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': post2['partitionKey'],
        'sortKey': 'trending',
        'schemaVersion': 0,
        'gsiK3PartitionKey': 'post/trending',
        'gsiK3SortKey': Decimal(random()).normalize(),
        'lastDeflatedAt': now_str,
        'createdAt': now_str,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def trending3(dynamo_table, post3):
    now_str = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': post3['partitionKey'],
        'sortKey': 'trending',
        'schemaVersion': 0,
        'gsiK3PartitionKey': 'post/trending',
        'gsiK3SortKey': Decimal(random()).normalize(),
        'lastDeflatedAt': now_str,
        'createdAt': now_str,
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_migrate_none_to_migrate(dynamo_client, dynamo_table, caplog, not_completed_post):
    # verify no trendings exist
    query_kwargs = {
        'KeyConditionExpression': 'gsiK3PartitionKey = :gsik3pk',
        'ExpressionAttributeValues': {':gsik3pk': 'post/trending'},
        'IndexName': 'GSI-K3',
    }
    resp = dynamo_table.query(**query_kwargs)
    assert resp['Items'] == []
    assert 'LastEvaluatedKey' not in resp

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify no trendings exist
    resp = dynamo_table.query(**query_kwargs)
    assert resp['Items'] == []
    assert 'LastEvaluatedKey' not in resp


def test_migrate_all_already_migrated(
    dynamo_client, dynamo_table, caplog, post1, post2, post3, trending1, trending2, trending3
):
    # verify three trendings exist
    query_kwargs = {
        'KeyConditionExpression': 'gsiK3PartitionKey = :gsik3pk',
        'ExpressionAttributeValues': {':gsik3pk': 'post/trending'},
        'IndexName': 'GSI-K3',
    }
    resp = dynamo_table.query(**query_kwargs)
    assert len(resp['Items']) == 3
    assert 'LastEvaluatedKey' not in resp

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify three trendings exist
    resp = dynamo_table.query(**query_kwargs)
    assert len(resp['Items']) == 3
    assert 'LastEvaluatedKey' not in resp


def test_migrate_one(dynamo_client, dynamo_table, caplog, post1):
    # check starting state
    post_id = post1['postId']
    trending_pk = {'partitionKey': post1['partitionKey'], 'sortKey': 'trending'}
    assert 'Item' not in dynamo_table.get_item(Key=trending_pk)

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        before = pendulum.now('utc')
        migration.run()
        after = pendulum.now('utc')
    assert len(caplog.records) == 1
    assert all('adding trending' in rec.msg for rec in caplog.records)
    assert all(post_id in rec.msg for rec in caplog.records)

    # check final state
    trending = dynamo_table.get_item(Key=trending_pk)['Item']
    assert trending.pop('partitionKey').split('/') == ['post', post_id]
    assert trending.pop('sortKey') == 'trending'
    assert trending.pop('schemaVersion') == 0
    assert before < pendulum.parse(trending.pop('lastDeflatedAt')) < after
    assert before < pendulum.parse(trending.pop('createdAt')) < after
    assert trending.pop('gsiK3PartitionKey') == 'post/trending'
    assert 0.5 < trending.pop('gsiK3SortKey') < 1


def test_race_condition_on_adding_new_trending(dynamo_client, dynamo_table, caplog, post1, trending1):
    # check starting state
    post_id = post1['postId']
    trending_pk = {'partitionKey': post1['partitionKey'], 'sortKey': 'trending'}
    assert dynamo_table.get_item(Key=trending_pk)['Item'] == trending1

    # run the add, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.add_trending(post_id, Decimal(random() / 2 + 0.5).normalize(), pendulum.now('utc'))
    assert len(caplog.records) == 2
    assert all('adding trending' in rec.msg for rec in caplog.records)
    assert all(post_id in rec.msg for rec in caplog.records)
    assert 'FAILED, skipping' in caplog.records[1].msg

    # check final state
    assert dynamo_table.get_item(Key=trending_pk)['Item'] == trending1


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, post1, post2, post3):
    # check starting state
    post_id1 = post1['postId']
    post_id2 = post2['postId']
    post_id3 = post3['postId']
    trending_pk1 = {'partitionKey': post1['partitionKey'], 'sortKey': 'trending'}
    trending_pk2 = {'partitionKey': post2['partitionKey'], 'sortKey': 'trending'}
    trending_pk3 = {'partitionKey': post3['partitionKey'], 'sortKey': 'trending'}
    assert 'Item' not in dynamo_table.get_item(Key=trending_pk1)
    assert 'Item' not in dynamo_table.get_item(Key=trending_pk2)
    assert 'Item' not in dynamo_table.get_item(Key=trending_pk3)

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert all('adding trending' in rec.msg for rec in caplog.records)
    assert sum(post_id1 in rec.msg for rec in caplog.records) == 1
    assert sum(post_id2 in rec.msg for rec in caplog.records) == 1
    assert sum(post_id3 in rec.msg for rec in caplog.records) == 1

    # check final state
    assert dynamo_table.get_item(Key=trending_pk1)
    assert dynamo_table.get_item(Key=trending_pk2)
    assert dynamo_table.get_item(Key=trending_pk3)
