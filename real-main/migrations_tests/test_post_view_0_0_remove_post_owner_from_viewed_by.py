from migrations.post_view_0_0_remove_post_owner_from_viewed_by import Migration


def test_basic(dynamo_client, dynamo_table):
    # create minimal a user
    user_id = 'uid'
    user_pk = {
        'partitionKey': f'user/{user_id}',
        'sortKey': 'profile',
    }
    dynamo_table.put_item(Item=user_pk)

    # create post 1 by that user
    post_id_1 = 'pid-1'
    post_pk_1 = {
        'partitionKey': f'post/{post_id_1}',
        'sortKey': '-',
    }
    dynamo_table.put_item(Item=post_pk_1)

    # create a postView by the post owner on post 1 (with counts on post and user)
    dynamo_table.put_item(
        Item={
            'partitionKey': f'postView/{post_id_1}/{user_id}',
            'sortKey': '-',
            'schemaVersion': 0,
            'postId': post_id_1,
            'postedByUserId': user_id,
            'viewedByUserId': user_id,
        },
    )
    dynamo_table.update_item(
        Key=post_pk_1,
        UpdateExpression='ADD viewedByCount :one',
        ExpressionAttributeValues={':one': 1},
    )
    dynamo_table.update_item(
        Key=user_pk,
        UpdateExpression='ADD postViewedByCount :one',
        ExpressionAttributeValues={':one': 1},
    )

    # create a postView by someone else on post 1 (with counts on post and user)
    other_user_id_1 = 'ouid-1'
    dynamo_table.put_item(
        Item={
            'partitionKey': f'postView/{post_id_1}/{other_user_id_1}',
            'sortKey': '-',
            'schemaVersion': 0,
            'postId': post_id_1,
            'postedByUserId': user_id,
            'viewedByUserId': other_user_id_1,
        },
    )
    dynamo_table.update_item(
        Key=post_pk_1,
        UpdateExpression='ADD viewedByCount :one',
        ExpressionAttributeValues={':one': 1},
    )
    dynamo_table.update_item(
        Key=user_pk,
        UpdateExpression='ADD postViewedByCount :one',
        ExpressionAttributeValues={':one': 1},
    )

    # create post 2 by same user
    post_id_2 = 'pid-2'
    post_pk_2 = {
        'partitionKey': f'post/{post_id_2}',
        'sortKey': '-',
    }
    dynamo_table.put_item(Item=post_pk_2)

    # create a postView by the post owner on post 2 (with counts on post and user)
    dynamo_table.put_item(
        Item={
            'partitionKey': f'postView/{post_id_2}/{user_id}',
            'sortKey': '-',
            'schemaVersion': 0,
            'postId': post_id_2,
            'postedByUserId': user_id,
            'viewedByUserId': user_id,
        },
    )
    dynamo_table.update_item(
        Key=post_pk_2,
        UpdateExpression='ADD viewedByCount :one',
        ExpressionAttributeValues={':one': 1},
    )
    dynamo_table.update_item(
        Key=user_pk,
        UpdateExpression='ADD postViewedByCount :one',
        ExpressionAttributeValues={':one': 1},
    )

    # create a postView by someone else on post 2 (with counts on post and user)
    other_user_id_2 = 'ouid-2'
    dynamo_table.put_item(
        Item={
            'partitionKey': f'postView/{post_id_2}/{other_user_id_2}',
            'sortKey': '-',
            'schemaVersion': 0,
            'postId': post_id_2,
            'postedByUserId': user_id,
            'viewedByUserId': other_user_id_2,
        },
    )
    dynamo_table.update_item(
        Key=post_pk_2,
        UpdateExpression='ADD viewedByCount :one',
        ExpressionAttributeValues={':one': 1},
    )
    dynamo_table.update_item(
        Key=user_pk,
        UpdateExpression='ADD postViewedByCount :one',
        ExpressionAttributeValues={':one': 1},
    )

    # check everything is as expected
    user = dynamo_table.get_item(Key=user_pk)['Item']
    post_1 = dynamo_table.get_item(Key=post_pk_1)['Item']
    post_2 = dynamo_table.get_item(Key=post_pk_2)['Item']
    post_views = dynamo_table.scan(
        FilterExpression='begins_with(partitionKey, :pk_prefix)',
        ExpressionAttributeValues={':pk_prefix': 'postView/'},
    )['Items']
    post_views_by_user = [pv for pv in post_views if pv['viewedByUserId'] == user_id]
    post_views_by_others = [pv for pv in post_views if pv['viewedByUserId'] != user_id]

    assert user['postViewedByCount'] == 4
    assert post_1['viewedByCount'] == 2
    assert post_2['viewedByCount'] == 2
    assert len(post_views) == 4
    assert len(post_views_by_user) == 2
    assert len(post_views_by_others) == 2

    # run main
    migration = Migration(dynamo_client, dynamo_table)
    migration.run()

    # check everything is as now expected
    user = dynamo_table.get_item(Key=user_pk)['Item']
    post_1 = dynamo_table.get_item(Key=post_pk_1)['Item']
    post_2 = dynamo_table.get_item(Key=post_pk_2)['Item']
    post_views = dynamo_table.scan(
        FilterExpression='begins_with(partitionKey, :pk_prefix)',
        ExpressionAttributeValues={':pk_prefix': 'postView/'},
    )['Items']
    post_views_by_user = [pv for pv in post_views if pv['viewedByUserId'] == user_id]
    post_views_by_others = [pv for pv in post_views if pv['viewedByUserId'] != user_id]

    assert user['postViewedByCount'] == 2
    assert post_1['viewedByCount'] == 1
    assert post_2['viewedByCount'] == 1
    assert len(post_views) == 2
    assert len(post_views_by_user) == 0
    assert len(post_views_by_others) == 2

    # run main again
    migration.run()

    # check nothing has changed
    user = dynamo_table.get_item(Key=user_pk)['Item']
    post_1 = dynamo_table.get_item(Key=post_pk_1)['Item']
    post_2 = dynamo_table.get_item(Key=post_pk_2)['Item']
    post_views = dynamo_table.scan(
        FilterExpression='begins_with(partitionKey, :pk_prefix)',
        ExpressionAttributeValues={':pk_prefix': 'postView/'},
    )['Items']
    post_views_by_user = [pv for pv in post_views if pv['viewedByUserId'] == user_id]
    post_views_by_others = [pv for pv in post_views if pv['viewedByUserId'] != user_id]

    assert user['postViewedByCount'] == 2
    assert post_1['viewedByCount'] == 1
    assert post_2['viewedByCount'] == 1
    assert len(post_views) == 2
    assert len(post_views_by_user) == 0
    assert len(post_views_by_others) == 2
