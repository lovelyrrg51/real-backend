import logging
from uuid import uuid4

import pytest

from migrations.post_3_2_reorg_verification_hidden import Migration


@pytest.fixture
def post_verification_not_hidden_1(dynamo_table):
    post_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
        'isVerified': False,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post_verification_not_hidden_2(dynamo_table):
    post_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
        'verificationHidden': False,
        'isVerified': False,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post_verification_hidden_1(dynamo_table):
    post_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
        'verificationHidden': True,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post_verification_hidden_2(dynamo_table):
    post_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
        'verificationHidden': True,
        'isVerified': True,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post_verification_hidden_3(dynamo_table):
    post_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
        'verificationHidden': True,
        'isVerified': False,
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_nothing_to_migrate(
    dynamo_client,
    dynamo_table,
    caplog,
    post_verification_not_hidden_1,
    post_verification_not_hidden_2,
    post_verification_hidden_1,
):
    post0 = post_verification_not_hidden_1
    post1 = post_verification_not_hidden_2
    post2 = post_verification_hidden_1
    key0 = {k: post0[k] for k in ('partitionKey', 'sortKey')}
    key1 = {k: post1[k] for k in ('partitionKey', 'sortKey')}
    key2 = {k: post2[k] for k in ('partitionKey', 'sortKey')}
    assert 'lastPostViewAt' not in post0
    assert 'lastPostViewAt' not in post1
    assert 'lastPostViewAt' not in post2
    assert dynamo_table.get_item(Key=key0)['Item'] == post0
    assert dynamo_table.get_item(Key=key1)['Item'] == post1
    assert dynamo_table.get_item(Key=key2)['Item'] == post2

    # do the migration, check post0 unchanged
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert dynamo_table.get_item(Key=key0)['Item'] == post0
    assert dynamo_table.get_item(Key=key1)['Item'] == post1
    assert dynamo_table.get_item(Key=key2)['Item'] == post2


@pytest.mark.parametrize(
    'post', pytest.lazy_fixture(['post_verification_hidden_2', 'post_verification_hidden_3'])
)
def test_migrate_one_post(dynamo_client, dynamo_table, caplog, post):
    key = {k: post[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == post
    assert post['verificationHidden'] is True
    assert 'isVerifiedHiddenValue' not in post

    # do the migration, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert all(x in caplog.records[0].msg for x in ['Migrating', post['postId']])

    # check final state
    new_post = dynamo_table.get_item(Key=key)['Item']
    assert new_post.pop('isVerified') is True
    assert new_post.pop('isVerifiedHiddenValue') is post.pop('isVerified')
    assert new_post == post


def test_migrate_multiple(
    dynamo_client, dynamo_table, caplog, post_verification_hidden_2, post_verification_hidden_3
):
    post0 = post_verification_hidden_2
    post1 = post_verification_hidden_3
    key0 = {k: post0[k] for k in ('partitionKey', 'sortKey')}
    key1 = {k: post1[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key0)['Item'] == post0
    assert dynamo_table.get_item(Key=key1)['Item'] == post1

    # do the migration, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 2
    assert sum(post0['postId'] in rec.msg for rec in caplog.records) == 1
    assert sum(post1['postId'] in rec.msg for rec in caplog.records) == 1
    assert sum('Migrating' in rec.msg for rec in caplog.records) == 2

    # check final state
    assert dynamo_table.get_item(Key=key0)['Item']['isVerified'] is True
    assert dynamo_table.get_item(Key=key1)['Item']['isVerified'] is True
    assert dynamo_table.get_item(Key=key0)['Item']['isVerifiedHiddenValue'] is post0['isVerified']
    assert dynamo_table.get_item(Key=key1)['Item']['isVerifiedHiddenValue'] is post1['isVerified']

    # check running again is a no-op
    caplog.clear()
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
