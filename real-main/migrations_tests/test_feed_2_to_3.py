import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.feed_2_to_3 import Migration


@pytest.fixture
def feed_item(dynamo_table):
    post_id = str(uuid4())
    posted_by_user_id = str(uuid4())
    user_id = str(uuid4())
    posted_at = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'feed/{user_id}',
        'schemaVersion': 2,
        'gsiA1PartitionKey': f'feed/{user_id}',
        'gsiA1SortKey': posted_at,
        'gsiA2PartitionKey': f'feed/{user_id}',
        'gsiA2SortKey': posted_by_user_id,
        'userId': user_id,
        'postId': post_id,
        'postedAt': posted_at,
        'postedByUserId': posted_by_user_id,
        'gsiK2PartitionKey': f'feed/{user_id}/{posted_by_user_id}',
        'gsiK2SortKey': posted_at,
    }
    dynamo_table.put_item(Item=item)
    yield item


a = feed_item
b = feed_item
c = feed_item
d = feed_item
e = feed_item
f = feed_item
g = feed_item
h = feed_item
i = feed_item
j = feed_item
k = feed_item
l = feed_item  # noqa: E741
m = feed_item
n = feed_item
o = feed_item
p = feed_item
q = feed_item
r = feed_item
s = feed_item
t = feed_item
u = feed_item
v = feed_item
w = feed_item
x = feed_item
y = feed_item
z = feed_item


@pytest.fixture
def feed_items(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z):  # noqa: E741
    yield [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z]


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog):
    # add something to the db to ensure it doesn't migrate
    pk = {'partitionKey': 'not-a-feed-item', 'sortKey': '-'}
    dynamo_table.put_item(Item=pk)
    assert dynamo_table.get_item(Key=pk)['Item'] == pk

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
    assert dynamo_table.get_item(Key=pk)['Item'] == pk


def test_migrate_one(dynamo_client, dynamo_table, caplog, feed_item):
    pk = {k: feed_item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=pk)['Item'] == feed_item
    post_id = pk['partitionKey'].split('/')[1]
    user_id = pk['sortKey'].split('/')[1]

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in str(caplog.records[0])
    assert post_id in str(caplog.records[0])
    assert user_id in str(caplog.records[0])

    # verify final state
    new_item = dynamo_table.get_item(Key=pk)['Item']
    assert new_item.pop('partitionKey') == f'post/{post_id}'
    assert new_item.pop('sortKey') == f'feed/{user_id}'
    assert new_item.pop('gsiA1PartitionKey') == f'feed/{user_id}'
    assert new_item.pop('gsiA1SortKey') == feed_item['postedAt']
    assert new_item.pop('gsiA2PartitionKey') == f'feed/{user_id}'
    assert new_item.pop('gsiA2SortKey') == feed_item['postedByUserId']
    assert new_item.pop('schemaVersion') == 3
    assert new_item == {}


def test_migrate_two_batches(dynamo_client, dynamo_table, caplog, feed_items):
    scan_kwargs = {
        'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND begins_with(sortKey, :sk_prefix)',
        'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sk_prefix': 'feed/'},
    }
    assert len(dynamo_table.scan(**scan_kwargs)['Items']) == 26
    for item in dynamo_table.scan(**scan_kwargs)['Items']:
        assert len(item) == 13

    # do the migration, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 2
    for f in feed_items[:25]:
        assert f['userId'] in caplog.records[0].msg
        assert f['postId'] in caplog.records[0].msg
    for f in feed_items[25:]:
        assert f['userId'] in caplog.records[1].msg
        assert f['postId'] in caplog.records[1].msg

    # do the migration, check final state
    assert len(dynamo_table.scan(**scan_kwargs)['Items']) == 26
    for item in dynamo_table.scan(**scan_kwargs)['Items']:
        assert len(item) == 7
        for field in (
            'partitionKey',
            'sortKey',
            'gsiA1PartitionKey',
            'gsiA1SortKey',
            'gsiA2PartitionKey',
            'gsiA2SortKey',
        ):
            assert field in item
        assert item['schemaVersion'] == 3

    # migrate again, check no-op
    caplog.clear()
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
