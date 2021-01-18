import functools
import logging

import pendulum
from boto3.dynamodb.conditions import Key

from ..exceptions import FollowerAlreadyHasStatus

logger = logging.getLogger()


class FollowerDynamo:
    def __init__(self, dynamo_client):
        self.client = dynamo_client

    def pk(self, follower_user_id, followed_user_id):
        return {
            'partitionKey': f'user/{followed_user_id}',
            'sortKey': f'follower/{follower_user_id}',
        }

    def get_following(self, follower_user_id, followed_user_id, strongly_consistent=False):
        pk = self.pk(follower_user_id, followed_user_id)
        return self.client.get_item(pk, ConsistentRead=strongly_consistent)

    def add_following(self, follower_user_id, followed_user_id, follow_status):
        followed_at_str = pendulum.now('utc').to_iso8601_string()
        query_kwargs = {
            'Item': {
                **self.pk(follower_user_id, followed_user_id),
                'schemaVersion': 1,
                'gsiA1PartitionKey': f'follower/{follower_user_id}',
                'gsiA1SortKey': f'{follow_status}/{followed_at_str}',
                'gsiA2PartitionKey': f'followed/{followed_user_id}',
                'gsiA2SortKey': f'{follow_status}/{followed_at_str}',
                'followedAt': followed_at_str,
                'followStatus': follow_status,
                'followerUserId': follower_user_id,
                'followedUserId': followed_user_id,
            },
        }
        try:
            return self.client.add_item(query_kwargs)
        except self.client.exceptions.ConditionalCheckFailedException as err:
            raise FollowerAlreadyHasStatus(follower_user_id, followed_user_id, follow_status) from err

    def update_following_status(self, follow_item, follow_status):
        key = {k: follow_item[k] for k in ('partitionKey', 'sortKey')}
        query_kwargs = {
            'Key': key,
            'UpdateExpression': 'SET followStatus = :status, gsiA1SortKey = :sk, gsiA2SortKey = :sk',
            'ExpressionAttributeValues': {
                ':status': follow_status,
                ':sk': f'{follow_status}/{follow_item["followedAt"]}',
            },
        }
        return self.client.update_item(query_kwargs)

    def delete_following(self, follow_item):
        key = {k: follow_item[k] for k in ('partitionKey', 'sortKey')}
        return self.client.delete_item(key)

    def generate_followed_items(self, user_id, follow_status=None, keys_only=False):
        "Generate items that represent a followed of the given user (that the given user is the follower)"
        key_conditions = [Key('gsiA1PartitionKey').eq(f'follower/{user_id}')]
        if follow_status is not None:
            key_conditions.append(Key('gsiA1SortKey').begins_with(follow_status + '/'))
        query_kwargs = {
            'KeyConditionExpression': functools.reduce(lambda a, b: a & b, key_conditions),
            'IndexName': 'GSI-A1',
        }
        if keys_only:
            query_kwargs['ProjectionExpression'] = 'partitionKey, sortKey'
        return self.client.generate_all_query(query_kwargs)

    def generate_follower_items(self, user_id, follow_status=None, keys_only=False):
        "Generate items that represent a follower of the given user (that the given user is the followed)"
        key_conditions = [Key('gsiA2PartitionKey').eq(f'followed/{user_id}')]
        if follow_status is not None:
            key_conditions.append(Key('gsiA2SortKey').begins_with(follow_status + '/'))
        query_kwargs = {
            'KeyConditionExpression': functools.reduce(lambda a, b: a & b, key_conditions),
            'IndexName': 'GSI-A2',
        }
        if keys_only:
            query_kwargs['ProjectionExpression'] = 'partitionKey, sortKey'
        return self.client.generate_all_query(query_kwargs)
