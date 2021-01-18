import logging

from migrations.post_1_to_2 import Migration


def test_basic(dynamo_client, dynamo_table, caplog):
    # create a minimal post
    post_id_1 = 'pid1'
    post_pk_1 = {
        'partitionKey': f'post/{post_id_1}',
        'sortKey': '-',
    }
    dynamo_table.put_item(
        Item={
            **post_pk_1,
            **{
                'postId': post_id_1,
                'postedByUserId': 'uid-1',
                'postedAt': 'p-at-1',
                'postStatus': 'p-s-1',
                'schemaVersion': 1,
            },
        }
    )

    # create another minimal post
    post_id_2 = 'pid2'
    post_pk_2 = {
        'partitionKey': f'post/{post_id_2}',
        'sortKey': '-',
    }
    dynamo_table.put_item(
        Item={
            **post_pk_2,
            **{
                'postId': post_id_2,
                'postedByUserId': 'uid-2',
                'postedAt': 'p-at-2',
                'postStatus': 'p-s-2',
                'schemaVersion': 1,
            },
        }
    )

    # check both posts looks as expected
    post_1 = dynamo_table.get_item(Key=post_pk_1)['Item']
    assert post_1['postId'] == post_id_1
    assert post_1['schemaVersion'] == 1
    assert 'postType' not in post_1
    assert 'gsiA3PartitionKey' not in post_1
    assert 'gsiA3SortKey' not in post_1

    post_2 = dynamo_table.get_item(Key=post_pk_2)['Item']
    assert post_2['postId'] == post_id_2
    assert post_2['schemaVersion'] == 1
    assert 'postType' not in post_2
    assert 'gsiA3PartitionKey' not in post_2
    assert 'gsiA3SortKey' not in post_2

    # run the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # check logging worked
    assert len(caplog.records) == 2
    assert post_id_1 in str(caplog.records[0])
    assert post_id_2 in str(caplog.records[1])

    # check everything is as now expected
    post_1 = dynamo_table.get_item(Key=post_pk_1)['Item']
    assert post_1['postId'] == post_id_1
    assert post_1['postType'] == 'IMAGE'
    assert post_1['schemaVersion'] == 2
    assert post_1['gsiA3PartitionKey'] == 'post/uid-1'
    assert post_1['gsiA3SortKey'] == 'p-s-1/IMAGE/p-at-1'

    post_2 = dynamo_table.get_item(Key=post_pk_2)['Item']
    assert post_2['postId'] == post_id_2
    assert post_2['schemaVersion'] == 2
    assert post_2['postType'] == 'IMAGE'
    assert post_2['gsiA3PartitionKey'] == 'post/uid-2'
    assert post_2['gsiA3SortKey'] == 'p-s-2/IMAGE/p-at-2'

    # run the migration again, should do nothing
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        migration.run()

    # check logging worked
    assert len(caplog.records) == 0

    # check nothing has changed
    post_1 = dynamo_table.get_item(Key=post_pk_1)['Item']
    assert post_1['postId'] == post_id_1
    assert post_1['schemaVersion'] == 2
    assert post_1['postType'] == 'IMAGE'
    assert post_1['gsiA3PartitionKey'] == 'post/uid-1'
    assert post_1['gsiA3SortKey'] == 'p-s-1/IMAGE/p-at-1'

    post_2 = dynamo_table.get_item(Key=post_pk_2)['Item']
    assert post_2['postId'] == post_id_2
    assert post_2['schemaVersion'] == 2
    assert post_2['postType'] == 'IMAGE'
    assert post_2['gsiA3PartitionKey'] == 'post/uid-2'
    assert post_2['gsiA3SortKey'] == 'p-s-2/IMAGE/p-at-2'
