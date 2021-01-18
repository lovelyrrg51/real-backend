import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.user_11_0_fill_last_post_view_at import Migration


@pytest.fixture
def user0(dynamo_table):
    user_id = f'us-east-1:{uuid4()}'
    item = {'partitionKey': f'user/{user_id}', 'sortKey': 'profile', 'userId': user_id}
    dynamo_table.put_item(Item=item)
    yield item


user1 = user0
user2 = user0


@pytest.fixture
def user1_post_view(dynamo_table, user1):
    post_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'view/{user1["userId"]}',
        'lastViewedAt': pendulum.now('utc').to_iso8601_string(),
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user2_post_view1(dynamo_table, user2):
    post_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'view/{user2["userId"]}',
        'lastViewedAt': pendulum.now('utc').to_iso8601_string(),
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def user2_post_view2(dynamo_table, user2, user2_post_view1):
    post_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'view/{user2["userId"]}',
        # strictly backward in time
        'lastViewedAt': (
            pendulum.parse(user2_post_view1['lastViewedAt']) - pendulum.duration(hours=1)
        ).to_iso8601_string(),
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog, user0, user1, user2):
    key0 = {k: user0[k] for k in ('partitionKey', 'sortKey')}
    key1 = {k: user1[k] for k in ('partitionKey', 'sortKey')}
    key2 = {k: user2[k] for k in ('partitionKey', 'sortKey')}
    assert 'lastPostViewAt' not in user0
    assert 'lastPostViewAt' not in user1
    assert 'lastPostViewAt' not in user2
    assert dynamo_table.get_item(Key=key0)['Item'] == user0
    assert dynamo_table.get_item(Key=key1)['Item'] == user1
    assert dynamo_table.get_item(Key=key2)['Item'] == user2

    # do the migration, check user0 unchanged
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert dynamo_table.get_item(Key=key0)['Item'] == user0
    assert dynamo_table.get_item(Key=key1)['Item'] == user1
    assert dynamo_table.get_item(Key=key2)['Item'] == user2


def test_migrate_one_post_view(dynamo_client, dynamo_table, caplog, user1, user1_post_view):
    key = {k: user1[k] for k in ('partitionKey', 'sortKey')}
    assert 'lastPostViewAt' not in user1
    assert dynamo_table.get_item(Key=key)['Item'] == user1

    # do the migration, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert all(x in caplog.records[0].msg for x in ['updated', user1['userId']])

    # check final state
    new_user1 = dynamo_table.get_item(Key=key)['Item']
    assert new_user1.pop('lastPostViewAt') == user1_post_view['lastViewedAt']
    assert new_user1 == user1


def test_migrate_two_post_view(dynamo_client, dynamo_table, caplog, user2, user2_post_view1, user2_post_view2):
    key = {k: user2[k] for k in ('partitionKey', 'sortKey')}
    assert 'lastPostViewAt' not in user2
    assert dynamo_table.get_item(Key=key)['Item'] == user2

    # do the migration, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 2
    assert all(user2['userId'] in rec.msg for rec in caplog.records)
    assert sum('updated' in rec.msg for rec in caplog.records) == 1
    assert sum('did not update' in rec.msg for rec in caplog.records) == 1

    # check final state
    new_user2 = dynamo_table.get_item(Key=key)['Item']
    assert new_user2.pop('lastPostViewAt') == max(
        user2_post_view1['lastViewedAt'], user2_post_view2['lastViewedAt']
    )
    assert new_user2 == user2


def test_migrate_multiple(
    dynamo_client, dynamo_table, caplog, user0, user1, user2, user1_post_view, user2_post_view1, user2_post_view2
):
    key0 = {k: user0[k] for k in ('partitionKey', 'sortKey')}
    key1 = {k: user1[k] for k in ('partitionKey', 'sortKey')}
    key2 = {k: user2[k] for k in ('partitionKey', 'sortKey')}
    assert 'lastPostViewAt' not in user0
    assert 'lastPostViewAt' not in user1
    assert 'lastPostViewAt' not in user2
    assert dynamo_table.get_item(Key=key0)['Item'] == user0
    assert dynamo_table.get_item(Key=key1)['Item'] == user1
    assert dynamo_table.get_item(Key=key2)['Item'] == user2

    # do the migration, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert sum(user1['userId'] in rec.msg for rec in caplog.records) == 1
    assert sum(user2['userId'] in rec.msg for rec in caplog.records) == 2
    assert sum('did not update' in rec.msg for rec in caplog.records) == 1
    assert sum('updated' in rec.msg for rec in caplog.records) == 2

    # check final state
    assert dynamo_table.get_item(Key=key0)['Item'] == user0
    assert dynamo_table.get_item(Key=key1)['Item']['lastPostViewAt'] == user1_post_view['lastViewedAt']
    assert dynamo_table.get_item(Key=key2)['Item']['lastPostViewAt'] == max(
        user2_post_view1['lastViewedAt'], user2_post_view2['lastViewedAt']
    )
