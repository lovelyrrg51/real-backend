import logging
import uuid

import pytest

from migrations.post_3_0_remove_gsi_a3 import Migration


@pytest.fixture
def post_already_migrated(dynamo_table):
    post_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post(dynamo_table):
    post_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
        'gsiA3PartitionKey': str(uuid.uuid4())[:8],
        'gsiA3SortKey': str(uuid.uuid4())[:8],
    }
    dynamo_table.put_item(Item=item)
    yield item


post2 = post
post3 = post


@pytest.fixture
def posts(post, post2, post3):
    yield [post, post2, post3]


@pytest.mark.parametrize('post', [pytest.lazy_fixture('post_already_migrated')])
def test_migrate_none(dynamo_client, dynamo_table, caplog, post):
    # check starting state
    post_pk = {k: post[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=post_pk)['Item'] == post

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # check final state
    post_pk = {k: post[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=post_pk)['Item'] == post


def test_migrate_one(dynamo_client, dynamo_table, caplog, post):
    # check starting state
    post_pk = {k: post[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=post_pk)['Item'] == post
    assert 'gsiA3PartitionKey' in post
    assert 'gsiA3SortKey' in post

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert post['postId'] in caplog.records[0].msg
    assert 'migrating' in caplog.records[0].msg

    # check final state
    post_pk = {k: post[k] for k in ('partitionKey', 'sortKey')}
    new_post = dynamo_table.get_item(Key=post_pk)['Item']
    assert 'gsiA3PartitionKey' not in new_post
    assert 'gsiA3SortKey' not in new_post
    assert post.pop('gsiA3PartitionKey')
    assert post.pop('gsiA3SortKey')
    assert new_post == post


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, posts):
    # check starting state
    post_pks = [{k: post[k] for k in ('partitionKey', 'sortKey')} for post in posts]
    post_keys_to_items = list(zip(post_pks, posts))
    assert all(dynamo_table.get_item(Key=post_pk)['Item'] == post for post_pk, post in post_keys_to_items)
    assert all('gsiA3PartitionKey' in post for post in posts)
    assert all('gsiA3SortKey' in post for post in posts)

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert sum(1 for rec in caplog.records if posts[0]['postId'] in rec.msg) == 1
    assert sum(1 for rec in caplog.records if posts[1]['postId'] in rec.msg) == 1
    assert sum(1 for rec in caplog.records if posts[2]['postId'] in rec.msg) == 1

    # check final state
    new_posts = [dynamo_table.get_item(Key=post_pk)['Item'] for post_pk, post in post_keys_to_items]
    assert all('gsiA3PartitionKey' not in post for post in new_posts)
    assert all('gsiA3SortKey' not in post for post in new_posts)
    assert all(post.pop('gsiA3PartitionKey') for post in posts)
    assert all(post.pop('gsiA3SortKey') for post in posts)
    assert new_posts == posts
