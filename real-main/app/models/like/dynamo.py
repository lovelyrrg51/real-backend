import functools
import logging

import pendulum
from boto3.dynamodb.conditions import Key

from .exceptions import AlreadyLiked, NotLikedWithStatus

logger = logging.getLogger()


class LikeDynamo:
    def __init__(self, dynamo_client):
        self.client = dynamo_client

    def pk(self, liked_by_user_id, post_id):
        return {'partitionKey': f'post/{post_id}', 'sortKey': f'like/{liked_by_user_id}'}

    def parse_pk(self, pk):
        if pk['sortKey'] == '-':
            _, liked_by_user_id, post_id = pk['partitionKey'].split('/')
        else:
            _, post_id = pk['partitionKey'].split('/')
            _, liked_by_user_id = pk['sortKey'].split('/')
        return liked_by_user_id, post_id

    def get_like(self, liked_by_user_id, post_id):
        return self.client.get_item(self.pk(liked_by_user_id, post_id))

    def add_like(self, liked_by_user_id, post_item, like_status, now=None):
        now = now or pendulum.now('utc')
        liked_at_str = now.to_iso8601_string()
        post_id = post_item['postId']
        posted_by_user_id = post_item['postedByUserId']

        query_kwargs = {
            'Item': {
                **self.pk(liked_by_user_id, post_id),
                'schemaVersion': 1,
                'gsiA1PartitionKey': f'like/{liked_by_user_id}',
                'gsiA1SortKey': f'{like_status}/{liked_at_str}',
                'gsiA2PartitionKey': f'like/{post_id}',
                'gsiA2SortKey': f'{like_status}/{liked_at_str}',
                'gsiK2PartitionKey': f'like/{posted_by_user_id}',
                'gsiK2SortKey': liked_by_user_id,
                'likedByUserId': liked_by_user_id,
                'likeStatus': like_status,
                'likedAt': liked_at_str,
                'postId': post_id,
            },
        }
        try:
            return self.client.add_item(query_kwargs)
        except self.client.exceptions.ConditionalCheckFailedException as err:
            raise AlreadyLiked(liked_by_user_id, post_id) from err

    def delete_like(self, liked_by_user_id, post_id, like_status):
        kwargs = {
            'ConditionExpression': 'likeStatus = :like_status',
            'ExpressionAttributeValues': {':like_status': like_status},
        }
        try:
            self.client.delete_item(self.pk(liked_by_user_id, post_id), **kwargs)
        except self.client.exceptions.ConditionalCheckFailedException as err:
            raise NotLikedWithStatus(liked_by_user_id, post_id, like_status) from err

    def generate_of_post(self, post_id):
        query_kwargs = {
            'KeyConditionExpression': Key('gsiA2PartitionKey').eq(f'like/{post_id}'),
            'IndexName': 'GSI-A2',
        }
        return self.client.generate_all_query(query_kwargs)

    def generate_by_liked_by(self, liked_by_user_id):
        query_kwargs = {
            'KeyConditionExpression': Key('gsiA1PartitionKey').eq(f'like/{liked_by_user_id}'),
            'IndexName': 'GSI-A1',
        }
        return self.client.generate_all_query(query_kwargs)

    def generate_pks_by_liked_by_for_posted_by(self, liked_by_user_id, posted_by_user_id):
        key_conditions = [
            Key('gsiK2PartitionKey').eq(f'like/{posted_by_user_id}'),
            Key('gsiK2SortKey').eq(liked_by_user_id),
        ]
        query_kwargs = {
            'KeyConditionExpression': functools.reduce(lambda a, b: a & b, key_conditions),
            'IndexName': 'GSI-K2',
            # Note: moto (mocking framework used in test suite) needs this projection expression,
            #       else it returns the whole item even though the dynamo index is keys-only
            'ProjectionExpression': 'partitionKey, sortKey',
        }
        return self.client.generate_all_query(query_kwargs)
