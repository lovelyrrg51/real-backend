import logging
from uuid import uuid4

import pendulum
import pytest

from migrations.comment_view_0_delete import Migration


@pytest.fixture
def distractions(dynamo_table):
    message_id = str(uuid4())
    user_id = str(uuid4())
    items = [
        {'partitionKey': f'comment/{message_id}', 'sortKey': f'flag/{user_id}', 'm': 2},
        {'partitionKey': f'post/{message_id}', 'sortKey': f'view/{user_id}', 'n': 4},
    ]
    for item in items:
        dynamo_table.put_item(Item=item)
    yield items


@pytest.fixture
def comment_view1(dynamo_table):
    message_id = str(uuid4())
    user_id = str(uuid4())
    now_str = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': f'comment/{message_id}',
        'sortKey': f'view/{user_id}',
        'schemaVersion': 0,
        'firstViewedAt': now_str,
        'lastViewedAt': now_str,
        'viewCount': 1,
        'gsiK1PartitionKey': f'comment/{message_id}',
        'gsiK1SortKey': f'view/{now_str}',
    }
    dynamo_table.put_item(Item=item)
    yield item


comment_view2 = comment_view1
comment_view3 = comment_view1


@pytest.fixture
def comment_views(comment_view1, comment_view2, comment_view3):
    yield [comment_view1, comment_view2, comment_view3]


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog, distractions):
    # verify starting state
    keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in distractions]
    for key, item in zip(keys, distractions):
        assert dynamo_table.get_item(Key=key)['Item'] == item

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify state has not changed
    for key, item in zip(keys, distractions):
        assert dynamo_table.get_item(Key=key)['Item'] == item


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, comment_views, distractions):
    dist_keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in distractions]
    cv_keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in comment_views]

    # check starting state
    for key, item in zip(dist_keys, distractions):
        assert dynamo_table.get_item(Key=key)['Item'] == item
    for key, item in zip(cv_keys, comment_views):
        assert dynamo_table.get_item(Key=key)['Item'] == item

    # do the migration, check state
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert sum('Deleting' in rec.msg for rec in caplog.records) == 3
    for cv in comment_views:
        assert sum(cv['partitionKey'] in rec.msg for rec in caplog.records) == 1
        assert sum(cv['sortKey'] in rec.msg for rec in caplog.records) == 1

    # check final state
    for key, item in zip(dist_keys, distractions):
        assert dynamo_table.get_item(Key=key)['Item'] == item
    for key in cv_keys:
        assert 'Item' not in dynamo_table.get_item(Key=key)
