import logging
import uuid

import pytest

from migrations.comment_0_to_1 import Migration


@pytest.fixture
def comment_already_migrated(dynamo_table):
    comment_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'comment/{comment_id}',
        'sortKey': '-',
        'schemaVersion': 1,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def comment_no_views(dynamo_table):
    comment_id = str(uuid.uuid4())
    item = {
        'partitionKey': f'comment/{comment_id}',
        'sortKey': '-',
        'schemaVersion': 0,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def comment_one_unrecorded_view(dynamo_table):
    comment_id = str(uuid.uuid4())
    view_item = {
        'partitionKey': f'comment/{comment_id}',
        'sortKey': f'view/{uuid.uuid4()}',
        'schemaVersion': 0,
    }
    dynamo_table.put_item(Item=view_item)
    item = {
        'partitionKey': f'comment/{comment_id}',
        'sortKey': '-',
        'schemaVersion': 0,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def comment_one_recorded_view(dynamo_table):
    comment_id = str(uuid.uuid4())
    view_item = {
        'partitionKey': f'comment/{comment_id}',
        'sortKey': f'view/{uuid.uuid4()}',
        'schemaVersion': 0,
    }
    dynamo_table.put_item(Item=view_item)
    item = {
        'partitionKey': f'comment/{comment_id}',
        'sortKey': '-',
        'schemaVersion': 0,
        'viewedByCount': 1,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def comment_multiple_views(dynamo_table):
    "Comment with some recorded and some unrecorded views"
    comment_id = str(uuid.uuid4())
    for _ in range(5):
        view_item = {
            'partitionKey': f'comment/{comment_id}',
            'sortKey': f'view/{uuid.uuid4()}',
            'schemaVersion': 0,
        }
        dynamo_table.put_item(Item=view_item)
    item = {
        'partitionKey': f'comment/{comment_id}',
        'sortKey': '-',
        'schemaVersion': 0,
        'viewedByCount': 2,
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_migrate_no_items(dynamo_table, caplog, comment_already_migrated):
    comment = comment_already_migrated
    key = {k: comment[k] for k in ('partitionKey', 'sortKey')}

    # do the migration, verify no logs
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify no changes
    assert dynamo_table.get_item(Key=key)['Item'] == comment


def test_migrate_comment_no_views(dynamo_table, caplog, comment_no_views):
    comment = comment_no_views
    key = {k: comment[k] for k in ('partitionKey', 'sortKey')}

    # do the migration, verify logs
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert key['partitionKey'] in str(caplog.records[0])
    assert str(caplog.records[0]).count('`None`') == 1
    assert str(caplog.records[0]).count('`0`') == 1

    # verify correct changes to item
    item = dynamo_table.get_item(Key=key)['Item']
    assert item.pop('schemaVersion') == 1
    assert 'viewedByCount' not in item
    assert comment.pop('schemaVersion') == 0
    assert item == comment


def test_migrate_comment_one_unrecorded_view(dynamo_table, caplog, comment_one_unrecorded_view):
    comment = comment_one_unrecorded_view
    key = {k: comment[k] for k in ('partitionKey', 'sortKey')}

    # do the migration, verify logs
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert key['partitionKey'] in str(caplog.records[0])
    assert str(caplog.records[0]).count('`None`') == 1
    assert str(caplog.records[0]).count('`1`') == 1

    # verify correct changes to item
    item = dynamo_table.get_item(Key=key)['Item']
    assert item.pop('schemaVersion') == 1
    assert item.pop('viewedByCount') == 1
    assert comment.pop('schemaVersion') == 0
    assert item == comment


def test_migrate_comment_one_recorded_view(dynamo_table, caplog, comment_one_recorded_view):
    comment = comment_one_recorded_view
    key = {k: comment[k] for k in ('partitionKey', 'sortKey')}

    # do the migration, verify logs
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert key['partitionKey'] in str(caplog.records[0])
    assert str(caplog.records[0]).count('`1`') == 2

    # verify correct changes to item
    item = dynamo_table.get_item(Key=key)['Item']
    assert item.pop('schemaVersion') == 1
    assert item.pop('viewedByCount') == 1
    assert comment.pop('schemaVersion') == 0
    assert comment.pop('viewedByCount') == 1
    assert item == comment


def test_migrate_comment_multiple_views(dynamo_table, caplog, comment_multiple_views):
    comment = comment_multiple_views
    key = {k: comment[k] for k in ('partitionKey', 'sortKey')}

    # do the migration, verify logs
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert key['partitionKey'] in str(caplog.records[0])
    assert str(caplog.records[0]).count('`2`') == 1
    assert str(caplog.records[0]).count('`5`') == 1

    # verify correct changes to item
    item = dynamo_table.get_item(Key=key)['Item']
    assert item.pop('schemaVersion') == 1
    assert item.pop('viewedByCount') == 5
    assert comment.pop('schemaVersion') == 0
    assert comment.pop('viewedByCount') == 2
    assert item == comment


def test_migrate_multiple_item(
    dynamo_table,
    caplog,
    comment_already_migrated,
    comment_no_views,
    comment_one_unrecorded_view,
    comment_one_recorded_view,
    comment_multiple_views,
):
    comment1 = comment_no_views
    comment2 = comment_one_unrecorded_view
    comment3 = comment_one_recorded_view
    comment4 = comment_multiple_views

    key1 = {k: comment1[k] for k in ('partitionKey', 'sortKey')}
    key2 = {k: comment2[k] for k in ('partitionKey', 'sortKey')}
    key3 = {k: comment3[k] for k in ('partitionKey', 'sortKey')}
    key4 = {k: comment4[k] for k in ('partitionKey', 'sortKey')}
    key_already = {k: comment_already_migrated[k] for k in ('partitionKey', 'sortKey')}

    # do the migration, verify logs
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 4
    assert key1['partitionKey'] in str(caplog.records[0])
    assert key2['partitionKey'] in str(caplog.records[1])
    assert key3['partitionKey'] in str(caplog.records[2])
    assert key4['partitionKey'] in str(caplog.records[3])

    # verify correct items changed
    assert dynamo_table.get_item(Key=key1)['Item'] != comment1
    assert dynamo_table.get_item(Key=key2)['Item'] != comment2
    assert dynamo_table.get_item(Key=key3)['Item'] != comment3
    assert dynamo_table.get_item(Key=key4)['Item'] != comment4
    assert dynamo_table.get_item(Key=key_already)['Item'] == comment_already_migrated

    # do the migration again, verify no logs
    caplog.clear()
    migration = Migration(dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
