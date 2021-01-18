import logging
import uuid
from unittest import mock

import pytest

from migrations.user_8_to_9_load_pinpoint_endpoints import Migration


@pytest.fixture
def user_already_migrated(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 9,
        'userId': user_id,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_with_no_phone_or_email(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 8,
        'userId': user_id,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_with_email(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 8,
        'userId': user_id,
        'email': 'user-with-email-test@real.app',
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_with_phone(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 8,
        'userId': user_id,
        'phoneNumber': '+12125551212',
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user_with_phone_and_email(dynamo_table):
    user_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
        'schemaVersion': 8,
        'userId': user_id,
        'email': 'another-user-with-email-test@real.app',
        'phoneNumber': '+14155551212',
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_migrate_no_users_to_migrate(dynamo_client, dynamo_table, pinpoint_client, caplog, user_already_migrated):
    user = user_already_migrated
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}

    # check starting state
    assert pinpoint_client.mock_calls == []
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user
    assert user['schemaVersion'] == 9

    # migrate
    migration = Migration(dynamo_client, dynamo_table, pinpoint_client, 'pnt-app-id')
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # check final state
    assert pinpoint_client.mock_calls == []
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user


def test_migrate_user_with_no_phone_or_email(
    dynamo_client, dynamo_table, pinpoint_client, caplog, user_with_no_phone_or_email
):
    user = user_with_no_phone_or_email
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}

    # check starting state
    assert pinpoint_client.mock_calls == []
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user
    assert user['schemaVersion'] == 8

    # migrate
    migration = Migration(dynamo_client, dynamo_table, pinpoint_client, 'pnt-app-id')
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 2
    assert all(user['userId'] in rec.msg for rec in caplog.records)
    assert 'starting migration' in caplog.records[0].msg
    assert 'migrated' in caplog.records[1].msg

    # check final state
    assert pinpoint_client.mock_calls == []
    new_user = dynamo_table.get_item(Key=user_pk)['Item']
    assert new_user.pop('schemaVersion') == 9
    assert user.pop('schemaVersion') == 8
    assert new_user == user


def test_migrate_user_with_email(dynamo_client, dynamo_table, pinpoint_client, caplog, user_with_email):
    user = user_with_email
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}

    # check starting state
    assert pinpoint_client.mock_calls == []
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user
    assert user['schemaVersion'] == 8

    # migrate
    migration = Migration(dynamo_client, dynamo_table, pinpoint_client, 'pnt-app-id')
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert all(user['userId'] in rec.msg for rec in caplog.records)
    assert 'starting migration' in caplog.records[0].msg
    assert all(x in caplog.records[1].msg for x in ('updating pinpoint', 'EMAIL', user['email']))
    assert 'migrated' in caplog.records[2].msg

    # check final state
    endpoint_id = pinpoint_client.update_endpoint.call_args.kwargs['EndpointId']
    assert str(uuid.UUID(endpoint_id)) == endpoint_id
    assert pinpoint_client.mock_calls == [
        mock.call.update_endpoint(
            **{
                'ApplicationId': 'pnt-app-id',
                'EndpointId': endpoint_id,
                'EndpointRequest': {
                    'Address': user['email'],
                    'ChannelType': 'EMAIL',
                    'User': {'UserId': user['userId']},
                },
            }
        )
    ]
    new_user = dynamo_table.get_item(Key=user_pk)['Item']
    assert new_user.pop('schemaVersion') == 9
    assert user.pop('schemaVersion') == 8
    assert new_user == user


def test_migrate_user_with_phone(dynamo_client, dynamo_table, pinpoint_client, caplog, user_with_phone):
    user = user_with_phone
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}

    # check starting state
    assert pinpoint_client.mock_calls == []
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user
    assert user['schemaVersion'] == 8

    # migrate
    migration = Migration(dynamo_client, dynamo_table, pinpoint_client, 'pnt-app-id')
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert all(user['userId'] in rec.msg for rec in caplog.records)
    assert 'starting migration' in caplog.records[0].msg
    assert all(x in caplog.records[1].msg for x in ('updating pinpoint', 'SMS', user['phoneNumber']))
    assert 'migrated' in caplog.records[2].msg

    # check final state
    endpoint_id = pinpoint_client.update_endpoint.call_args.kwargs['EndpointId']
    assert str(uuid.UUID(endpoint_id)) == endpoint_id
    assert pinpoint_client.mock_calls == [
        mock.call.update_endpoint(
            **{
                'ApplicationId': 'pnt-app-id',
                'EndpointId': endpoint_id,
                'EndpointRequest': {
                    'Address': user['phoneNumber'],
                    'ChannelType': 'SMS',
                    'User': {'UserId': user['userId']},
                },
            }
        )
    ]
    new_user = dynamo_table.get_item(Key=user_pk)['Item']
    assert new_user.pop('schemaVersion') == 9
    assert user.pop('schemaVersion') == 8
    assert new_user == user


def test_migrate_user_with_phone_and_email(
    dynamo_client, dynamo_table, pinpoint_client, caplog, user_with_phone_and_email
):
    user = user_with_phone_and_email
    user_pk = {k: user[k] for k in ('partitionKey', 'sortKey')}

    # check starting state
    assert pinpoint_client.mock_calls == []
    assert dynamo_table.get_item(Key=user_pk)['Item'] == user
    assert user['schemaVersion'] == 8

    # migrate
    migration = Migration(dynamo_client, dynamo_table, pinpoint_client, 'pnt-app-id')
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 4
    assert all(user['userId'] in rec.msg for rec in caplog.records)
    assert 'starting migration' in caplog.records[0].msg
    assert all(x in caplog.records[1].msg for x in ('updating pinpoint', 'EMAIL', user['email']))
    assert all(x in caplog.records[2].msg for x in ('updating pinpoint', 'SMS', user['phoneNumber']))
    assert user['phoneNumber'] in caplog.records[2].msg
    assert 'migrated' in caplog.records[3].msg

    # check final state
    endpoint_id_1 = pinpoint_client.update_endpoint.call_args_list[0].kwargs['EndpointId']
    endpoint_id_2 = pinpoint_client.update_endpoint.call_args_list[1].kwargs['EndpointId']
    assert str(uuid.UUID(endpoint_id_1)) == endpoint_id_1
    assert str(uuid.UUID(endpoint_id_2)) == endpoint_id_2
    assert pinpoint_client.mock_calls == [
        mock.call.update_endpoint(
            **{
                'ApplicationId': 'pnt-app-id',
                'EndpointId': endpoint_id_1,
                'EndpointRequest': {
                    'Address': user['email'],
                    'ChannelType': 'EMAIL',
                    'User': {'UserId': user['userId']},
                },
            }
        ),
        mock.call.update_endpoint(
            **{
                'ApplicationId': 'pnt-app-id',
                'EndpointId': endpoint_id_2,
                'EndpointRequest': {
                    'Address': user['phoneNumber'],
                    'ChannelType': 'SMS',
                    'User': {'UserId': user['userId']},
                },
            }
        ),
    ]
    new_user = dynamo_table.get_item(Key=user_pk)['Item']
    assert new_user.pop('schemaVersion') == 9
    assert user.pop('schemaVersion') == 8
    assert new_user == user


def test_migrate_multiple(
    dynamo_client,
    dynamo_table,
    pinpoint_client,
    caplog,
    user_with_no_phone_or_email,
    user_with_email,
    user_with_phone,
    user_with_phone_and_email,
):
    users = [user_with_no_phone_or_email, user_with_email, user_with_phone, user_with_phone_and_email]
    user_pks = [{k: user[k] for k in ('partitionKey', 'sortKey')} for user in users]
    user_keys_to_items = list(zip(user_pks, users))

    # check starting state
    assert pinpoint_client.mock_calls == []
    assert all(dynamo_table.get_item(Key=user_pk)['Item'] == user for user_pk, user in user_keys_to_items)
    assert all(user['schemaVersion'] == 8 for user in users)

    # migrate
    migration = Migration(dynamo_client, dynamo_table, pinpoint_client, 'pnt-app-id')
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 2 + 3 + 3 + 4
    assert sum(1 for rec in caplog.records if users[0]['userId'] in rec.msg) == 2
    assert sum(1 for rec in caplog.records if users[1]['userId'] in rec.msg) == 3
    assert sum(1 for rec in caplog.records if users[2]['userId'] in rec.msg) == 3
    assert sum(1 for rec in caplog.records if users[3]['userId'] in rec.msg) == 4

    # check calls to pinpoint
    call_args_list = pinpoint_client.update_endpoint.call_args_list
    assert len(call_args_list) == 4
    assert call_args_list[0].kwargs['EndpointRequest']['User']['UserId'] == users[1]['userId']
    assert call_args_list[1].kwargs['EndpointRequest']['User']['UserId'] == users[2]['userId']
    assert call_args_list[2].kwargs['EndpointRequest']['User']['UserId'] == users[3]['userId']
    assert call_args_list[3].kwargs['EndpointRequest']['User']['UserId'] == users[3]['userId']

    # check final state in dynamo
    new_users = [dynamo_table.get_item(Key=user_pk)['Item'] for user_pk, user in user_keys_to_items]
    assert all(user.pop('schemaVersion') == 9 for user in new_users)
    assert all(user.pop('schemaVersion') == 8 for user in users)
    assert new_users == users
