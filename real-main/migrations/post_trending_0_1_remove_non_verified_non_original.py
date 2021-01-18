import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "Remove post trendings for non-verified and non-original posts"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for post_pk in self.generate_trending_post_pks():
            post = self.get_post(post_pk)
            post_type = post.get('postType', 'IMAGE')
            is_verified = post.get('isVerified')
            post_id = post_pk.split('/')[1]
            original_post_id = post.get('originalPostId', post_id)
            if post_type == 'IMAGE' and not (is_verified and original_post_id == post_id):
                logging.warning(f'Post `{post_pk}`: deleting trending')
                self.delete_post_trending(post_pk)
            else:
                logging.warning(f'Post `{post_pk}`: leaving trending')

    def generate_trending_post_pks(self):
        query_kwargs = {
            'KeyConditionExpression': 'gsiK3PartitionKey = :gsik3pk',
            'ExpressionAttributeValues': {':gsik3pk': 'post/trending'},
            'IndexName': 'GSI-K3',
        }
        while True:
            paginated = self.dynamo_table.query(**query_kwargs)
            for item in paginated['Items']:
                yield item['partitionKey']
            if 'LastEvaluatedKey' not in paginated:
                break
            query_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def get_post(self, post_pk):
        key = {'partitionKey': post_pk, 'sortKey': '-'}
        return self.dynamo_table.get_item(Key=key)['Item']

    def delete_post_trending(self, post_pk):
        key = {'partitionKey': post_pk, 'sortKey': 'trending'}
        self.dynamo_table.delete_item(Key=key)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_client = boto3.client('dynamodb')
    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
