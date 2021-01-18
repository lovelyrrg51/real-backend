import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    """
    Move postView items to new post-child view items.
    """

    def __init__(self, boto_client, boto_table):
        self.boto_client = boto_client
        self.boto_table = boto_table
        self.table_name = boto_table.name
        self.operations = {
            'Put': self.boto_client.put_item,
            'Delete': self.boto_client.delete_item,
            'Update': self.boto_client.update_item,
        }

    def run(self):
        for post_view in self.generate_all_post_views():
            self.migrate_post_view(post_view)

    def generate_all_post_views(self):
        "Return a generator of all items in the table that pass the filter"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix)',
            'ExpressionAttributeValues': {':pk_prefix': 'postView/'},
        }
        while True:
            paginated = self.boto_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_post_view(self, post_view, iteration=0):
        post_id = post_view['postId']
        user_id = post_view['viewedByUserId']
        view_pk = {
            'partitionKey': f'post/{post_id}',
            'sortKey': f'view/{user_id}',
        }
        post_view_pk = {
            'partitionKey': f'postView/{post_id}/{user_id}',
            'sortKey': '-',
        }

        if iteration > 5:
            raise Exception(f'Unable to migration post view `{post_view_pk}` after `{iteration}` iterations')

        logger.warning(f'Migrating post view `{post_view_pk["partitionKey"]}`')

        # pull any existing view item from DB
        view = self.boto_table.get_item(Key=view_pk).get('Item', {})
        all_views = [post_view]
        if view:
            exp_vals = {':ovc': {'N': str(view['viewCount'])}}
            cond_exp = 'attribute_exists(partitionKey) and viewCount = :ovc'
            all_views.append(view)
        else:
            exp_vals = None
            cond_exp = 'attribute_not_exists(partitionKey)'

        # compute the new view item
        view_count = sum(v['viewCount'] for v in all_views)
        first_viewed_at_str = min(v['firstViewedAt'] for v in all_views)
        last_viewed_at_str = max(v['lastViewedAt'] for v in all_views)

        transact_put = {
            'Put': {
                'Item': {
                    'partitionKey': {'S': f'post/{post_id}'},
                    'sortKey': {'S': f'view/{user_id}'},
                    'gsiK1PartitionKey': {'S': f'post/{post_id}'},
                    'gsiK1SortKey': {'S': f'view/{first_viewed_at_str}'},
                    'schemaVersion': {'N': '0'},
                    'viewCount': {'N': str(view_count)},
                    'firstViewedAt': {'S': first_viewed_at_str},
                    'lastViewedAt': {'S': last_viewed_at_str},
                },
                'ConditionExpression': cond_exp,
                'TableName': self.table_name,
            }
        }
        if exp_vals:
            transact_put['Put']['ExpressionAttributeValues'] = exp_vals

        transact_delete = {
            'Delete': {
                'Key': {'partitionKey': {'S': f'postView/{post_id}/{user_id}'}, 'sortKey': {'S': '-'}},
                'ConditionExpression': 'attribute_exists(partitionKey)',
                'TableName': self.table_name,
            }
        }

        # set the view item and delete the post_view item in one trasaction
        expected_exceptions = (
            self.boto_client.exceptions.TransactionCanceledException,  # real dynamo table
            self.boto_client.exceptions.ConditionalCheckFailedException,  # moto
        )
        try:
            self.transact_write_items([transact_put, transact_delete])
        except expected_exceptions:
            logger.warning(f'Migrating post view `{post_view_pk["partitionKey"]}` failed. Trying aagin.')
            return self.migrate_post_view(post_view, iteration + 1)

    def transact_write_items(self, transacts):
        logger.info(f'Applying transaction: {transacts}')

        try:
            self.boto_client.transact_write_items(TransactItems=transacts)
        except AttributeError:
            # we're running under moto, ie, in the test suite
            for transact in transacts:
                assert len(transact) == 1
                key, kwargs = next(iter(transact.items()))

                if key == 'ConditionCheck':
                    # There is no corresponding operation we can do here, AFAIK
                    # Thus we can't test write failures due to ConditionChecks in test suite
                    continue

                assert key in self.operations
                self.operations[key](**kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    boto_client = boto3.client('dynamodb')

    migration = Migration(boto_client, boto_table)
    migration.run()
