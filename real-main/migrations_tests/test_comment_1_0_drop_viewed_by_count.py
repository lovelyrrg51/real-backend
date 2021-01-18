import logging
from random import randrange
from uuid import uuid4

import pendulum
import pytest

from migrations.comment_1_0_drop_viewed_by_count import Migration


@pytest.fixture
def distractions(dynamo_table):
    comment_id = str(uuid4())
    items = [
        {'partitionKey': f'comment/{comment_id}', 'sortKey': '--', 'viewedByCount': 2},
        {'partitionKey': f'post/{comment_id}', 'sortKey': '-', 'viewedByCount': 4},
        {'partitionKey': f'comment/{comment_id}', 'sortKey': '-'},  # comment without viewedByCount
    ]
    for item in items:
        dynamo_table.put_item(Item=item)
    yield items


@pytest.fixture
def comment(dynamo_table):
    comment_id, post_id, user_id = str(uuid4()), str(uuid4()), str(uuid4())
    commented_at_str = pendulum.now('utc').to_iso8601_string()
    item = {
        'partitionKey': f'comment/{comment_id}',
        'sortKey': '-',
        'schemaVersion': 1,
        'gsiA1PartitionKey': f'comment/{post_id}',
        'gsiA1SortKey': commented_at_str,
        'gsiA2PartitionKey': f'comment/{user_id}',
        'gsiA2SortKey': commented_at_str,
        'commentId': comment_id,
        'postId': post_id,
        'userId': user_id,
        'commentedAt': commented_at_str,
        'text': 'lore',
        'textTags': [],
        'viewedByCount': randrange(100),
    }
    dynamo_table.put_item(Item=item)
    yield item


comment1 = comment
comment2 = comment
comment3 = comment


@pytest.fixture
def comments(comment1, comment2, comment3):
    yield [comment1, comment2, comment3]


def test_nothing_to_migrate(dynamo_client, dynamo_table, caplog, distractions):
    keys = [{k: item[k] for k in ('partitionKey', 'sortKey')} for item in distractions]

    # verify starting state
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


def test_migrate_one(dynamo_client, dynamo_table, caplog, comment):
    key = {k: comment[k] for k in ('partitionKey', 'sortKey')}
    attrs_removed = ['viewedByCount']
    new_comment = {k: comment[k] for k in comment.keys() if k not in attrs_removed}

    # verify starting state
    assert dynamo_table.get_item(Key=key)['Item'] == comment
    assert new_comment != comment

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 1
    assert 'Migrating' in str(caplog.records[0])
    assert str(key) in str(caplog.records[0])

    # verify final state
    assert dynamo_table.get_item(Key=key)['Item'] == new_comment


def test_migrate_multiple(dynamo_client, dynamo_table, caplog, comments):
    keys = [{k: comment[k] for k in ('partitionKey', 'sortKey')} for comment in comments]
    attrs_removed = ['viewedByCount']
    new_comments = [{k: comment[k] for k in comment.keys() if k not in attrs_removed} for comment in comments]

    # verify starting state
    for key, comment, new_comment in zip(keys, comments, new_comments):
        assert dynamo_table.get_item(Key=key)['Item'] == comment
        assert comment != new_comment

    # do the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 3
    assert all('Migrating' in str(rec) for rec in caplog.records)
    for key in keys:
        assert sum(str(key) in str(rec) for rec in caplog.records) == 1

    # verify final state
    for key, new_comment in zip(keys, new_comments):
        assert dynamo_table.get_item(Key=key)['Item'] == new_comment

    # migrate again, verify no affect
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0

    # verify final state
    for key, new_comment in zip(keys, new_comments):
        assert dynamo_table.get_item(Key=key)['Item'] == new_comment
