import logging

import pendulum
from boto3.dynamodb.conditions import Key

from .exceptions import AlreadyBlocked

logger = logging.getLogger()


class BlockDynamo:
    def __init__(self, dynamo_client):
        self.client = dynamo_client

    def pk(self, blocker_user_id, blocked_user_id):
        return {'partitionKey': f'user/{blocked_user_id}', 'sortKey': f'blocker/{blocker_user_id}'}

    def get_block(self, blocker_user_id, blocked_user_id):
        return self.client.get_item(self.pk(blocker_user_id, blocked_user_id))

    def add_block(self, blocker_user_id, blocked_user_id, now=None):
        now = now or pendulum.now('utc')
        blocked_at_str = now.to_iso8601_string()
        query_kwargs = {
            'Item': {
                **self.pk(blocker_user_id, blocked_user_id),
                'schemaVersion': 0,
                'gsiA1PartitionKey': f'block/{blocker_user_id}',
                'gsiA1SortKey': blocked_at_str,
                'gsiA2PartitionKey': f'block/{blocked_user_id}',
                'gsiA2SortKey': blocked_at_str,
                'blockerUserId': blocker_user_id,
                'blockedUserId': blocked_user_id,
                'blockedAt': blocked_at_str,
            },
        }
        try:
            return self.client.add_item(query_kwargs)
        except self.client.exceptions.ConditionalCheckFailedException as err:
            raise AlreadyBlocked(blocker_user_id, blocked_user_id) from err

    def delete_block(self, blocker_user_id, blocked_user_id):
        return self.client.delete_item(self.pk(blocker_user_id, blocked_user_id))

    def generate_blocks_by_blocker(self, blocker_user_id):
        query_kwargs = {
            'KeyConditionExpression': Key('gsiA1PartitionKey').eq(f'block/{blocker_user_id}'),
            'IndexName': 'GSI-A1',
        }
        return self.client.generate_all_query(query_kwargs)

    def generate_blocks_by_blocked(self, blocked_user_id):
        query_kwargs = {
            'KeyConditionExpression': Key('gsiA2PartitionKey').eq(f'block/{blocked_user_id}'),
            'IndexName': 'GSI-A2',
        }
        return self.client.generate_all_query(query_kwargs)

    def delete_all_blocks_by_user(self, blocker_user_id):
        key_generator = (
            self.pk(blocker_user_id, block_item['blockedUserId'])
            for block_item in self.generate_blocks_by_blocker(blocker_user_id)
        )
        self.client.batch_delete_items(key_generator)

    def delete_all_blocks_of_user(self, blocked_user_id):
        key_generator = (
            self.pk(block_item['blockerUserId'], blocked_user_id)
            for block_item in self.generate_blocks_by_blocked(blocked_user_id)
        )
        self.client.batch_delete_items(key_generator)
