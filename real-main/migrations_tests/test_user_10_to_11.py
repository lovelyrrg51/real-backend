import logging
from random import randint
from uuid import uuid4

import pytest

from migrations.user_10_to_11 import Migration


@pytest.fixture
def user_with_neither(dynamo_table):
    user_id = str(uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 10,
        'userId': user_id,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_with_email(dynamo_table):
    user_id = str(uuid4())
    email = str(uuid4())[:8] + '@example.com'
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 10,
        'userId': user_id,
        'email': email,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_with_phone(dynamo_table):
    user_id = str(uuid4())
    phone_number = '+1' + ''.join(str(randint(1, 9) for _ in range(9)))
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 10,
        'userId': user_id,
        'phoneNumber': phone_number,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_with_both(dynamo_table):
    user_id = str(uuid4())
    email = str(uuid4())[:8] + '@example.com'
    phone_number = '+1' + ''.join(str(randint(1, 9) for _ in range(9)))
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 10,
        'userId': user_id,
        'email': email,
        'phoneNumber': phone_number,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_with_subitems(dynamo_table):
    user_id = str(uuid4())
    email = str(uuid4())[:8] + '@example.com'
    phone_number = '+1' + ''.join(str(randint(1, 9) for _ in range(9)))
    user_item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 10,
        'userId': user_id,
        'email': email,
        'phoneNumber': phone_number,
    }
    email_item = {'partitionKey': f'userEmail/{email}', 'sortKey': '-', 'schemaVersion': 0, 'userId': user_id}
    phone_item = {
        'partitionKey': f'userPhoneNumber/{phone_number}',
        'sortKey': '-',
        'schemaVersion': 0,
        'userId': user_id,
    }
    dynamo_table.put_item(Item=user_item)
    dynamo_table.put_item(Item=email_item)
    dynamo_table.put_item(Item=phone_item)
    yield (user_item, email_item, phone_item)


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog):
    # add something to the db to ensure it doesn't migrate
    key = {'partitionKey': 'unrelated-item', 'sortKey': '-'}
    dynamo_table.put_item(Item=key)
    assert dynamo_table.get_item(Key=key)['Item'] == key

    # do the migration, check unrelated item was not affected
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert dynamo_table.get_item(Key=key)['Item'] == key


@pytest.mark.parametrize(
    'user_item',
    pytest.lazy_fixture(['user_with_neither', 'user_with_email', 'user_with_phone', 'user_with_both']),
)
def test_migrate_one(dynamo_client, dynamo_table, caplog, user_item):
    user_id = user_item.get('userId')
    email = user_item.get('email')
    phone = user_item.get('phoneNumber')
    user_key = {k: user_item[k] for k in ('partitionKey', 'sortKey')}
    email_key = {'partitionKey': f'userEmail/{email}', 'sortKey': '-'} if email else None
    phone_key = {'partitionKey': f'userPhoneNumber/{phone}', 'sortKey': '-'} if phone else None
    assert dynamo_table.get_item(Key=user_key)['Item'] == user_item
    if email_key:
        assert 'Item' not in dynamo_table.get_item(Key=email_key)
    if phone_key:
        assert 'Item' not in dynamo_table.get_item(Key=phone_key)

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in str(caplog.records[0])
    assert user_id in str(caplog.records[0])
    assert 'update User.schemaVersion' in str(caplog.records[0])
    if email:
        assert 'add userEmail' in str(caplog.records[0])
    if phone:
        assert 'add userPhoneNumber' in str(caplog.records[0])

    # verify final state
    assert dynamo_table.get_item(Key=user_key)['Item'] == {**user_item, 'schemaVersion': 11}
    if email_key:
        item = dynamo_table.get_item(Key=email_key)['Item']
        assert item.pop('partitionKey').split('/') == ['userEmail', email]
        assert item.pop('sortKey') == '-'
        assert item.pop('schemaVersion') == 0
        assert item.pop('userId') == user_id
        assert item == {}
    if phone_key:
        item = dynamo_table.get_item(Key=phone_key)['Item']
        assert item.pop('partitionKey').split('/') == ['userPhoneNumber', phone]
        assert item.pop('sortKey') == '-'
        assert item.pop('schemaVersion') == 0
        assert item.pop('userId') == user_id
        assert item == {}


def test_migrate_already_with_subitems(dynamo_client, dynamo_table, caplog, user_with_subitems):
    user_item, email_item, phone_item = user_with_subitems
    user_id = user_item['userId']
    user_key = {k: user_item[k] for k in ('partitionKey', 'sortKey')}
    email_key = {k: email_item[k] for k in ('partitionKey', 'sortKey')}
    phone_key = {k: phone_item[k] for k in ('partitionKey', 'sortKey')}
    assert email_key['partitionKey'].split('/')[1] == user_item['email']
    assert phone_key['partitionKey'].split('/')[1] == user_item['phoneNumber']
    assert dynamo_table.get_item(Key=user_key)['Item'] == user_item
    assert dynamo_table.get_item(Key=email_key)['Item'] == email_item
    assert dynamo_table.get_item(Key=phone_key)['Item'] == phone_item

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in str(caplog.records[0])
    assert user_id in str(caplog.records[0])
    assert 'update User.schemaVersion' in str(caplog.records[0])
    assert 'add userEmail' in str(caplog.records[0])
    assert 'add userPhoneNumber' in str(caplog.records[0])

    # verify final state
    assert dynamo_table.get_item(Key=user_key)['Item'] == {**user_item, 'schemaVersion': 11}

    item = dynamo_table.get_item(Key=email_key)['Item']
    assert item.pop('partitionKey').split('/') == ['userEmail', user_item['email']]
    assert item.pop('sortKey') == '-'
    assert item.pop('schemaVersion') == 0
    assert item.pop('userId') == user_id
    assert item == {}

    item = dynamo_table.get_item(Key=phone_key)['Item']
    assert item.pop('partitionKey').split('/') == ['userPhoneNumber', user_item['phoneNumber']]
    assert item.pop('sortKey') == '-'
    assert item.pop('schemaVersion') == 0
    assert item.pop('userId') == user_id
    assert item == {}


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, user_with_email, user_with_phone, user_with_both):
    # check starting state
    scan_old_users_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND schemaVersion = :sv',
        'ExpressionAttributeValues': {':pk_prefix': 'user/', ':sv': 10},
    }
    scan_new_users_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND schemaVersion = :sv',
        'ExpressionAttributeValues': {':pk_prefix': 'user/', ':sv': 11},
    }
    scan_user_emails_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'userEmail/'},
    }
    scan_user_phones_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'userPhoneNumber/'},
    }
    assert len(dynamo_table.scan(**scan_old_users_kwargs)['Items']) == 3
    assert len(dynamo_table.scan(**scan_new_users_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**scan_user_emails_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**scan_user_phones_kwargs)['Items']) == 0

    # do the migration, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert all('Migrating' in str(rec) for rec in caplog.records)
    assert sum(1 for rec in caplog.records if 'update User.schemaVersion' in str(rec)) == 3
    assert sum(1 for rec in caplog.records if 'add userEmail' in str(rec)) == 2
    assert sum(1 for rec in caplog.records if 'add userPhoneNumber' in str(rec)) == 2

    # check state
    assert len(dynamo_table.scan(**scan_old_users_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**scan_new_users_kwargs)['Items']) == 3
    assert len(dynamo_table.scan(**scan_user_emails_kwargs)['Items']) == 2
    assert len(dynamo_table.scan(**scan_user_phones_kwargs)['Items']) == 2

    # migrate again, check logging implies no-op
    caplog.clear()
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # check state again
    assert len(dynamo_table.scan(**scan_old_users_kwargs)['Items']) == 0
    assert len(dynamo_table.scan(**scan_new_users_kwargs)['Items']) == 3
    assert len(dynamo_table.scan(**scan_user_emails_kwargs)['Items']) == 2
    assert len(dynamo_table.scan(**scan_user_phones_kwargs)['Items']) == 2
