import logging

from migrations.post_2_0_fix_posts_in_albums import Migration


def test_basic(dynamo_client, dynamo_table, caplog):
    # create a post in an album with the 'old' key format

    post_id = 'pid1'
    album_id = 'aid1'
    post_pk = {
        'partitionKey': f'post/{post_id}',
        'sortKey': '-',
    }
    dynamo_table.put_item(
        Item={
            **post_pk,
            **{
                'postId': post_id,
                'schemaVersion': 2,
                'albumId': album_id,
                'gsiK2PartitionKey': f'post/{album_id}',
                'gsiK2SortKey': 'post/posted-at-str',
            },
        }
    )

    # check the post looks as expected
    post_1 = dynamo_table.get_item(Key=post_pk)['Item']
    assert post_1['postId'] == post_id
    assert 'gsiK2PartitionKey' in post_1
    assert 'gsiK2SortKey' in post_1
    assert 'gsiK3PartitionKey' not in post_1
    assert 'gsiK3SortKey' not in post_1

    # run the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # check logging worked
    assert len(caplog.records) == 1
    assert post_id in str(caplog.records[0])

    # check everything is as now expected
    post_1 = dynamo_table.get_item(Key=post_pk)['Item']
    assert post_1['postId'] == post_id
    assert 'gsiK2PartitionKey' not in post_1
    assert 'gsiK2SortKey' not in post_1
    assert post_1['gsiK3PartitionKey'] == f'post/{album_id}'
    assert post_1['gsiK3SortKey']

    # run the migration again, should do nothing
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0
