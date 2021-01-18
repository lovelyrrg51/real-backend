import logging
import uuid

import pendulum
import pytest

from migrations.post_3_1_fill_gsi_a3 import Migration


@pytest.fixture
def post_no_need_to_migrate(dynamo_table):
    user_id, post_id = str(uuid.uuid4()), str(uuid.uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
        'postedByUserId': user_id,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post_already_migrated(dynamo_table):
    user_id, post_id = str(uuid.uuid4()), str(uuid.uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
        'postedByUserId': user_id,
        'gsiA3PartitionKey': f'post/{user_id}',
        'gsiA3SortKey': pendulum.now('utc').to_iso8601_string(),
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post_has_new_comment_activity(dynamo_table):
    user_id, post_id = str(uuid.uuid4()), str(uuid.uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
        'postedByUserId': user_id,
        'hasNewCommentActivity': True,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post_has_no_new_comment_activity(dynamo_table):
    user_id, post_id = str(uuid.uuid4()), str(uuid.uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
        'postId': post_id,
        'postedByUserId': user_id,
        'hasNewCommentActivity': False,
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_migrate_none(dynamo_client, dynamo_table, caplog, post_already_migrated, post_no_need_to_migrate):
    posts = [post_already_migrated, post_no_need_to_migrate]

    # check starting state
    post_pks = [{k: post[k] for k in ('partitionKey', 'sortKey')} for post in posts]
    post_keys_to_items = list(zip(post_pks, posts))
    assert all(dynamo_table.get_item(Key=post_pk)['Item'] == post for post_pk, post in post_keys_to_items)

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # check final state
    assert all(dynamo_table.get_item(Key=post_pk)['Item'] == post for post_pk, post in post_keys_to_items)


def test_migrate_has_no_new_comment_activity(
    dynamo_client, dynamo_table, caplog, post_has_no_new_comment_activity
):
    post = post_has_no_new_comment_activity

    # check starting state
    post_pk = {k: post[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=post_pk)['Item'] == post
    assert post['hasNewCommentActivity'] is False
    assert 'gsiA3PartitionKey' not in post
    assert 'gsiA3SortKey' not in post

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert post['postId'] in caplog.records[0].msg
    assert 'removing' in caplog.records[0].msg
    assert 'filling' not in caplog.records[0].msg

    # check final state
    post_pk = {k: post[k] for k in ('partitionKey', 'sortKey')}
    new_post = dynamo_table.get_item(Key=post_pk)['Item']
    assert 'gsiA3PartitionKey' not in new_post
    assert 'gsiA3SortKey' not in new_post
    assert 'hasNewCommentActivity' not in new_post
    assert post.pop('hasNewCommentActivity') is False
    assert new_post == post


def test_migrate_has_new_comment_activity(dynamo_client, dynamo_table, caplog, post_has_new_comment_activity):
    post = post_has_new_comment_activity

    # check starting state
    post_pk = {k: post[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=post_pk)['Item'] == post
    assert post['hasNewCommentActivity'] is True
    assert 'gsiA3PartitionKey' not in post
    assert 'gsiA3SortKey' not in post

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert post['postId'] in caplog.records[0].msg
    assert 'removing' in caplog.records[0].msg
    assert 'filling' in caplog.records[0].msg

    # check final state
    post_pk = {k: post[k] for k in ('partitionKey', 'sortKey')}
    new_post = dynamo_table.get_item(Key=post_pk)['Item']
    assert new_post.pop('gsiA3PartitionKey').split('/') == ['post', new_post['postedByUserId']]
    assert pendulum.parse(new_post.pop('gsiA3SortKey')) == migration.now
    assert 'hasNewCommentActivity' not in new_post
    assert post.pop('hasNewCommentActivity')
    assert new_post == post


post1 = post_has_new_comment_activity
post2 = post_has_no_new_comment_activity
post3 = post_has_new_comment_activity
post4 = post_has_no_new_comment_activity


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, post1, post2, post3, post4):
    posts = [post1, post2, post3, post4]

    # check starting state
    post_pks = [{k: post[k] for k in ('partitionKey', 'sortKey')} for post in posts]
    post_keys_to_items = list(zip(post_pks, posts))
    assert all(dynamo_table.get_item(Key=post_pk)['Item'] == post for post_pk, post in post_keys_to_items)
    assert all('gsiA3PartitionKey' not in post for post in posts)
    assert all('gsiA3SortKey' not in post for post in posts)
    assert sum(post['hasNewCommentActivity'] is True for post in posts) == 2
    assert sum(post['hasNewCommentActivity'] is False for post in posts) == 2

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 4
    assert sum(1 for rec in caplog.records if 'removing' in rec.msg) == 4
    assert sum(1 for rec in caplog.records if 'filling' in rec.msg) == 2
    assert sum(1 for rec in caplog.records if posts[0]['postId'] in rec.msg) == 1
    assert sum(1 for rec in caplog.records if posts[1]['postId'] in rec.msg) == 1
    assert sum(1 for rec in caplog.records if posts[2]['postId'] in rec.msg) == 1
    assert sum(1 for rec in caplog.records if posts[3]['postId'] in rec.msg) == 1

    # check final state
    new_posts = [dynamo_table.get_item(Key=post_pk)['Item'] for post_pk, post in post_keys_to_items]
    assert sum('hasNewCommentActivity' not in post for post in new_posts) == 4
    assert sum(post.pop('gsiA3PartitionKey', None) is not None for post in new_posts) == 2
    assert sum(post.pop('gsiA3SortKey', None) is not None for post in new_posts) == 2
    assert sum(post.pop('hasNewCommentActivity', None) is not None for post in posts) == 4
    assert new_posts == posts
