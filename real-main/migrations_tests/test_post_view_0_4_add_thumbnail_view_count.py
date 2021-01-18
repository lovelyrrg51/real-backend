import logging
from uuid import uuid4

import pytest

from migrations.post_view_0_4_add_thumbnail_view_count import Migration


@pytest.fixture
def post_view_no_change_needed(dynamo_table):
    post_id = str(uuid4())
    user_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'view/{user_id}',
        'schemaVersion': 0,
        'viewCount': 2,
        'thumbnailViewCount': 1,
        'focusViewCount': 1,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post_view_1(dynamo_table):
    post_id = str(uuid4())
    user_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'view/{user_id}',
        'schemaVersion': 0,
        'viewCount': 3,
        'focusViewCount': 1,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post_view_2(dynamo_table):
    post_id = str(uuid4())
    user_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'view/{user_id}',
        'schemaVersion': 0,
        'viewCount': 3,
    }
    dynamo_table.put_item(Item=item)
    yield item


@pytest.fixture
def post_view_3(dynamo_table):
    post_id = str(uuid4())
    user_id = str(uuid4())
    item = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'view/{user_id}',
        'schemaVersion': 0,
        'viewCount': 3,
        'focusViewCount': 1,
        'thumbnailViewCount': 1,
    }
    dynamo_table.put_item(Item=item)
    yield item


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog, post_view_no_change_needed):
    # create a distration in the DB
    key_post = {k: post_view_no_change_needed[k] for k in ('partitionKey', 'sortKey')}
    key_dist = {'partitionKey': f'post/{uuid4()}', 'sortKey': '-'}
    dynamo_table.put_item(Item=key_dist)
    assert dynamo_table.get_item(Key=key_dist)['Item'] == key_dist
    assert dynamo_table.get_item(Key=key_post)['Item'] == post_view_no_change_needed

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify final state
    assert dynamo_table.get_item(Key=key_dist)['Item'] == key_dist
    assert dynamo_table.get_item(Key=key_post)['Item'] == post_view_no_change_needed


@pytest.mark.parametrize('post_view', pytest.lazy_fixture(['post_view_1', 'post_view_2', 'post_view_3']))
def test_migrate_one(dynamo_client, dynamo_table, caplog, post_view):
    # verify starting state
    item = post_view
    post_id = item['partitionKey'].split('/')[1]
    user_id = item['sortKey'].split('/')[1]
    key = {k: item[k] for k in ('partitionKey', 'sortKey')}
    assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in caplog.records[0].msg
    assert post_id in caplog.records[0].msg
    assert user_id in caplog.records[0].msg

    # verify final state
    new_item = dynamo_table.get_item(Key=key)['Item']
    assert new_item == {
        **post_view,
        'thumbnailViewCount': post_view['viewCount'] - post_view.get('focusViewCount', 0),
    }


def test_race_condition_thumbnail_view_recorded(dynamo_client, dynamo_table, post_view_3):
    # make in-memory version represent one less thumbnail view that the DB representation
    post_view_3['viewCount'] -= 1
    post_view_3['thumbnailViewCount'] -= 1
    migration = Migration(dynamo_client, dynamo_table)
    with pytest.raises(dynamo_client.exceptions.ConditionalCheckFailedException):
        migration.migrate_post_view(post_view_3)


def test_race_condition_focus_view_recorded(dynamo_client, dynamo_table, post_view_3):
    # make in-memory version represent one less thumbnail view that the DB representation
    post_view_3['viewCount'] -= 1
    post_view_3['focusViewCount'] -= 1
    migration = Migration(dynamo_client, dynamo_table)
    with pytest.raises(dynamo_client.exceptions.ConditionalCheckFailedException):
        migration.migrate_post_view(post_view_3)


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, post_view_1, post_view_2, post_view_3):
    items = [post_view_1, post_view_2, post_view_3]

    # verify starting state
    keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in items]
    for key, item in zip(keys, items):
        assert dynamo_table.get_item(Key=key)['Item'] == item

    # migrate, check logging
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3

    # verify final state
    for key, item in zip(keys, items):
        new_item = dynamo_table.get_item(Key=key)['Item']
        assert new_item == {
            **item,
            'thumbnailViewCount': item['viewCount'] - item.get('focusViewCount', 0),
        }

    # migrate again, test no-op
    caplog.clear()
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
