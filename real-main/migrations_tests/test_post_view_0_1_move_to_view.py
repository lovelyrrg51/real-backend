import logging

import pendulum
import pytest

from migrations.post_view_0_1_move_to_view import Migration


def test_migrate_no_post_views(dynamo_client, dynamo_table, caplog):
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()
    assert len(caplog.records) == 0


def test_one_post_view_creates_new_view(dynamo_client, dynamo_table, caplog):
    # create a post view to migrate
    post_id = 'pid-xx'
    user_id = 'uid-yy'
    view_count = 42
    first_viewed_at_str = pendulum.now('utc').to_iso8601_string()
    last_viewed_at_str = pendulum.now('utc').to_iso8601_string()
    post_view_pk = {
        'partitionKey': f'postView/{post_id}/{user_id}',
        'sortKey': '-',
    }
    dynamo_table.put_item(
        Item={
            **post_view_pk,
            **{
                'schemaVersion': 0,
                'gsiA1PartitionKey': f'postView/{post_id}',
                'gsiA1SortKey': '{last_viewed_at_str}',
                'postId': post_id,
                'postedByUserId': 'uid-zz',
                'viewedByUserId': user_id,
                'viewCount': view_count,
                'firstViewedAt': first_viewed_at_str,
                'lastViewedAt': last_viewed_at_str,
            },
        }
    )

    # check we see the old post view
    assert dynamo_table.get_item(Key=post_view_pk)['Item']['postId'] == post_id

    # check we do not see the new view
    view_pk = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'view/{user_id}',
    }
    assert 'Item' not in dynamo_table.get_item(Key=view_pk)

    # run the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # check logging worked
    assert len(caplog.records) == 1
    assert post_id in str(caplog.records[0])
    assert user_id in str(caplog.records[0])

    # check the post view has disappeared
    assert 'Item' not in dynamo_table.get_item(Key=post_view_pk)

    # check the new view exists, with the correct format
    view = dynamo_table.get_item(Key=view_pk)['Item']
    assert view == {
        **view_pk,
        **{
            'schemaVersion': 0,
            'gsiK1PartitionKey': f'post/{post_id}',
            'gsiK1SortKey': f'view/{first_viewed_at_str}',
            'viewCount': view_count,
            'firstViewedAt': first_viewed_at_str,
            'lastViewedAt': last_viewed_at_str,
        },
    }


# using fixed datetimes here to play nice with running tests in parallel with pytest-xdist
at_1_str = pendulum.datetime(2020, 4, 15, 16, 17, 27, 582351).to_iso8601_string()
at_2_str = pendulum.datetime(2020, 4, 15, 16, 17, 27, 586571).to_iso8601_string()
at_3_str = pendulum.datetime(2020, 4, 15, 16, 17, 27, 592468).to_iso8601_string()
at_4_str = pendulum.datetime(2020, 4, 15, 16, 17, 27, 596673).to_iso8601_string()


@pytest.mark.parametrize("pv_first_viewed_at_str", [at_1_str, at_2_str])
@pytest.mark.parametrize("v_first_viewed_at_str", [at_1_str, at_2_str])
@pytest.mark.parametrize("pv_last_viewed_at_str", [at_3_str, at_4_str])
@pytest.mark.parametrize("v_last_viewed_at_str", [at_3_str, at_4_str])
def test_one_post_view_updates_existing_view(
    dynamo_client,
    dynamo_table,
    caplog,
    pv_first_viewed_at_str,
    v_first_viewed_at_str,
    pv_last_viewed_at_str,
    v_last_viewed_at_str,
):

    # create a post view to migrate
    post_id = 'pid-xx'
    user_id = 'uid-yy'
    pv_view_count = 42
    post_view_pk = {
        'partitionKey': f'postView/{post_id}/{user_id}',
        'sortKey': '-',
    }
    dynamo_table.put_item(
        Item={
            **post_view_pk,
            **{
                'schemaVersion': 0,
                'gsiA1PartitionKey': f'postView/{post_id}',
                'gsiA1SortKey': '{last_viewed_at_str}',
                'postId': post_id,
                'postedByUserId': 'uid-zz',
                'viewedByUserId': user_id,
                'viewCount': pv_view_count,
                'firstViewedAt': pv_first_viewed_at_str,
                'lastViewedAt': pv_last_viewed_at_str,
            },
        }
    )

    # check we see the old post view
    assert dynamo_table.get_item(Key=post_view_pk)['Item']['postId'] == post_id

    # create a view to update
    # check we do not see the new view
    v_view_count = 23
    view_pk = {
        'partitionKey': f'post/{post_id}',
        'sortKey': f'view/{user_id}',
    }
    dynamo_table.put_item(
        Item={
            **view_pk,
            **{
                'schemaVersion': 0,
                'gsiK1PartitionKey': f'post/{post_id}',
                'gsiK1SortKey': f'view/{user_id}',
                'viewCount': v_view_count,
                'firstViewedAt': v_first_viewed_at_str,
                'lastViewedAt': v_last_viewed_at_str,
            },
        }
    )

    # check we see the existing view
    assert dynamo_table.get_item(Key=view_pk)['Item']['partitionKey'] == f'post/{post_id}'

    # run the migration
    migration = Migration(dynamo_client, dynamo_table)
    with caplog.at_level(logging.WARNING):
        migration.run()

    # check logging worked
    assert len(caplog.records) == 1
    assert post_id in str(caplog.records[0])
    assert user_id in str(caplog.records[0])

    # check the post view has disappeared
    assert 'Item' not in dynamo_table.get_item(Key=post_view_pk)

    # check the new view exists, with the correct format
    view = dynamo_table.get_item(Key=view_pk)['Item']
    first_viewed_at_str = at_1_str if at_1_str in (pv_first_viewed_at_str, v_first_viewed_at_str) else at_2_str
    last_viewed_at_str = at_4_str if at_4_str in (pv_last_viewed_at_str, v_last_viewed_at_str) else at_3_str
    assert view == {
        **view_pk,
        **{
            'schemaVersion': 0,
            'gsiK1PartitionKey': f'post/{post_id}',
            'gsiK1SortKey': f'view/{first_viewed_at_str}',
            'viewCount': pv_view_count + v_view_count,
            'firstViewedAt': first_viewed_at_str,
            'lastViewedAt': last_viewed_at_str,
        },
    }
