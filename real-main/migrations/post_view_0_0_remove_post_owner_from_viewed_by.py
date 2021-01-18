import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    def __init__(self, boto_client, boto_table):
        self.boto_client = boto_client
        self.boto_table = boto_table
        self.table_name = boto_table.name

    def run(self):
        for post_view_item in self.generate_all_post_views(0):
            if post_view_item['viewedByUserId'] == post_view_item['postedByUserId']:
                self.clear_post_view(post_view_item)

    def generate_all_post_views(self, version):
        "Return a generator of all items in the table that pass the filter"
        assert isinstance(version, int)
        scan_kwargs = {
            'FilterExpression': ('begins_with(partitionKey, :pk_prefix) and schemaVersion = :sv'),
            'ExpressionAttributeValues': {':pk_prefix': 'postView/', ':sv': version},
        }
        while True:
            paginated = self.boto_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def clear_post_view(self, post_view_item):
        user_id = post_view_item['postedByUserId']
        post_id = post_view_item['postId']
        transacts = [
            # Delete the PostView
            {
                'Delete': {
                    'Key': {'partitionKey': {'S': f'postView/{post_id}/{user_id}'}, 'sortKey': {'S': '-'}},
                    'ConditionExpression': 'attribute_exists(partitionKey)',
                    'TableName': self.table_name,
                }
            },
            # Decrement the Post.piewedByCount
            {
                'Update': {
                    'Key': {'partitionKey': {'S': f'post/{post_id}'}, 'sortKey': {'S': '-'}},
                    'UpdateExpression': 'ADD viewedByCount :negative_one',
                    'ConditionExpression': 'attribute_exists(viewedByCount) and viewedByCount > :zero',
                    'ExpressionAttributeValues': {':negative_one': {'N': '-1'}, ':zero': {'N': '0'}},
                    'TableName': self.table_name,
                }
            },
            # Decrement the User.postViewedByCount
            {
                'Update': {
                    'Key': {'partitionKey': {'S': f'user/{user_id}'}, 'sortKey': {'S': 'profile'}},
                    'UpdateExpression': 'ADD postViewedByCount :negative_one',
                    'ConditionExpression': 'attribute_exists(postViewedByCount) and postViewedByCount > :zero',
                    'ExpressionAttributeValues': {':negative_one': {'N': '-1'}, ':zero': {'N': '0'}},
                    'TableName': self.table_name,
                }
            },
        ]
        logger.warning(f'Clearing post view for post `{post_id}`')
        self.boto_client.transact_write_items(TransactItems=transacts)


if __name__ == '__main__':
    if not DYNAMO_TABLE:
        raise Exception("Must set env variable DYNAMO_TABLE to dynamo table name")

    boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    boto_client = boto3.client('dynamodb')

    migration = Migration(boto_client, boto_table)
    migration.run()
