import logging
import random
import uuid

import pytest

from migrations.user_9_0_remove_posts_activity_counter import Migration


@pytest.fixture
def user_already_migrated(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'userId': user_id,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'userId': user_id,
        'postHasNewCommentActivityCount': random.randint(1, 100),
    }
    dynamo_table.put_item(Item=item)
    yield item


user2 = user
user3 = user


@pytest.fixture
def users(user, user2, user3):
    yield [user, user2, user3]


@pytest.mark.parametrize('user', [pytest.lazy_fixture('user_already_migrated')])
def test_migrate_none(dynamo_client, dynamo_table, caplog, user):
    # check starting state
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # check final state
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user


def test_migrate_one(dynamo_client, dynamo_table, caplog, user):
    # check starting state
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user
    assert 'postHasNewCommentActivityCount' in user

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert user['userId'] in caplog.records[0].msg
    assert 'migrating' in caplog.records[0].msg

    # check final state
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}
    new_user = dynamo_table.get_item(Key=user_pk)['Item']
    assert 'postHasNewCommentActivityCount' not in new_user
    assert user.pop('postHasNewCommentActivityCount')
    assert new_user == user


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, users):
    # check starting state
    user_pks = [{k: user[k] for k in ('partitionKey', 'sortKey')} for user in users]
    user_keys_to_items = list(zip(user_pks, users))
    assert all(dynamo_table.get_item(Key=user_pk)['Item'] == user for user_pk, user in user_keys_to_items)
    assert all('postHasNewCommentActivityCount' in user for user in users)

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert sum(1 for rec in caplog.records if users[0]['userId'] in rec.msg) == 1
    assert sum(1 for rec in caplog.records if users[1]['userId'] in rec.msg) == 1
    assert sum(1 for rec in caplog.records if users[2]['userId'] in rec.msg) == 1

    # check final state
    new_users = [dynamo_table.get_item(Key=user_pk)['Item'] for user_pk, user in user_keys_to_items]
    assert all('postHasNewCommentActivityCount' not in user for user in new_users)
    assert all(user.pop('postHasNewCommentActivityCount') for user in users)
    assert new_users == users
