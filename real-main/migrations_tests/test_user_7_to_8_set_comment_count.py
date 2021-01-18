import logging
import uuid

import pendulum
import pytest

from migrations.user_7_to_8_set_comment_count import Migration


@pytest.fixture
def user_with_none(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 7,
        'userId': user_id,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_with_zero(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 7,
        'userId': user_id,
        'commentCount': 0,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_with_two(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 7,
        'userId': user_id,
        'commentCount': 2,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_already_migrated(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 8,
        'userId': user_id,
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_migrate_no_users_to_migrate(dynamo_client, dynamo_table, caplog, user_already_migrated):
    user = user_already_migrated
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}

    # check starting state
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user
    assert user['schemaVersion'] == 8

    # migrate
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # check final state
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user


def test_migrate_user_with_none(dynamo_client, dynamo_table, caplog, user_with_none):
    user = user_with_none
    user_id = user['userId']
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}

    # check statrting state
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user
    assert user['schemaVersion'] == 7
    assert 'commentCount' not in user

    # migrate
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # verify logging
    assert len(caplog.records) == 4
    for rec in caplog.records:
        assert user_id in rec.msg
    assert 'starting migration' in caplog.records[0].msg
    assert 'counting comments' in caplog.records[1].msg
    assert 'updating user item' in caplog.records[2].msg
    assert 'finished migration' in caplog.records[3].msg

    # check final state
    user_updated = dynamo_table.get_item(Key=user_pk)['Item']
    assert user_updated['schemaVersion'] == 8
    assert 'commentCount' not in user_updated
    user['schemaVersion'] = user_updated['schemaVersion']
    assert user == user_updated


def test_migrate_user_with_zero(dynamo_client, dynamo_table, caplog, user_with_zero):
    user = user_with_zero
    user_id = user['userId']
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}

    # put one comment in the DB for this user
    for _ in range(0, 1):
        comment_id = str(uuid.uuid4())
        item = {
            'partitionKey': f'comment/{comment_id}',
            'sortKey': '-',
            'gsiA2PartitionKey': f'comment/{user_id}',
            'gsiA2SortKey': pendulum.now('utc').to_iso8601_string(),
        }
        dynamo_table.put_item(Item=item)

    # check statrting state
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user
    assert user['schemaVersion'] == 7
    assert user['commentCount'] == 0

    # migrate
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # verify logging
    assert len(caplog.records) == 4
    for rec in caplog.records:
        assert user_id in rec.msg
    assert 'starting migration' in caplog.records[0].msg
    assert 'counting comments' in caplog.records[1].msg
    assert 'updating user item' in caplog.records[2].msg
    assert 'finished migration' in caplog.records[3].msg

    # check final state
    user_updated = dynamo_table.get_item(Key=user_pk)['Item']
    assert user_updated['schemaVersion'] == 8
    assert user_updated['commentCount'] == 1
    user['schemaVersion'] = user_updated['schemaVersion']
    user['commentCount'] = user_updated['commentCount']
    assert user == user_updated


def test_migrate_user_with_two(dynamo_client, dynamo_table, caplog, user_with_two):
    user = user_with_two
    user_id = user['userId']
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}

    # put three comments in the DB for this user
    for _ in range(0, 3):
        comment_id = str(uuid.uuid4())
        item = {
            'partitionKey': f'comment/{comment_id}',
            'sortKey': '-',
            'gsiA2PartitionKey': f'comment/{user_id}',
            'gsiA2SortKey': pendulum.now('utc').to_iso8601_string(),
        }
        dynamo_table.put_item(Item=item)

    # check statrting state
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user
    assert user['schemaVersion'] == 7
    assert user['commentCount'] == 2

    # migrate
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # verify logging
    assert len(caplog.records) == 4
    for rec in caplog.records:
        assert user_id in rec.msg
    assert 'starting migration' in caplog.records[0].msg
    assert 'counting comments' in caplog.records[1].msg
    assert 'updating user item' in caplog.records[2].msg
    assert 'finished migration' in caplog.records[3].msg

    # check final state
    user_updated = dynamo_table.get_item(Key=user_pk)['Item']
    assert user_updated['schemaVersion'] == 8
    assert user_updated['commentCount'] == 3
    user['schemaVersion'] = user_updated['schemaVersion']
    user['commentCount'] = user_updated['commentCount']
    assert user == user_updated


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, user_with_none, user_with_two):
    user_1 = user_with_none
    user_2 = user_with_two

    user_id_1 = user_1['userId']
    user_id_2 = user_2['userId']

    user_pk_1 = {k: user_1[k] for k in ('partitionKey', 'sortKey')}
    user_pk_2 = {k: user_2[k] for k in ('partitionKey', 'sortKey')}

    # check initial state
    assert dynamo_table.get_item(Key=user_pk_1)['Item']['schemaVersion'] == 7
    assert dynamo_table.get_item(Key=user_pk_2)['Item']['schemaVersion'] == 7

    # migrate
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # verify logging
    assert len(caplog.records) == 8
    assert sum(1 for rec in caplog.records if user_id_1 in rec.msg) == 4
    assert sum(1 for rec in caplog.records if user_id_2 in rec.msg) == 4

    # check final state
    assert dynamo_table.get_item(Key=user_pk_1)['Item']['schemaVersion'] == 8
    assert dynamo_table.get_item(Key=user_pk_2)['Item']['schemaVersion'] == 8


def test_race_condition_comment_added(dynamo_client, dynamo_table, user_with_two):
    migration = Migration(dynamo_client, dynamo_table)

    with pytest.raises(Exception, match='Update failed for user'):
        migration.dynamo_update_user(user_with_two['userId'], None, 2)

    with pytest.raises(Exception, match='Update failed for user'):
        migration.dynamo_update_user(user_with_two['userId'], 0, 2)

    with pytest.raises(Exception, match='Update failed for user'):
        migration.dynamo_update_user(user_with_two['userId'], 1, 2)

    # no error
    migration.dynamo_update_user(user_with_two['userId'], 2, 2)
