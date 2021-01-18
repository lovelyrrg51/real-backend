import collections
import logging

import pendulum
from boto3.dynamodb.conditions import Key

from .exceptions import AlbumAlreadyExists, AlbumDoesNotExist

logger = logging.getLogger()


class AlbumDynamo:
    def __init__(self, dynamo_client):
        self.client = dynamo_client

    def pk(self, album_id):
        return {
            'partitionKey': f'album/{album_id}',
            'sortKey': '-',
        }

    def get_album(self, album_id, strongly_consistent=False):
        return self.client.get_item(self.pk(album_id), ConsistentRead=strongly_consistent)

    def add_album(self, album_id, user_id, name, description=None, created_at=None):
        created_at = created_at or pendulum.now('utc')
        created_at_str = created_at.to_iso8601_string()
        query_kwargs = {
            'Item': {
                **self.pk(album_id),
                'schemaVersion': 0,
                'gsiA1PartitionKey': f'album/{user_id}',
                'gsiA1SortKey': created_at_str,
                'albumId': album_id,
                'ownedByUserId': user_id,
                'createdAt': created_at_str,
                'name': name,
            },
        }
        if description is not None:
            query_kwargs['Item']['description'] = description
        try:
            return self.client.add_item(query_kwargs)
        except self.client.exceptions.ConditionalCheckFailedException as err:
            raise AlbumAlreadyExists(album_id) from err

    def set(self, album_id, name=None, description=None):
        assert name is not None or description is not None, 'Action-less post edit requested'
        assert name != '', 'All albums must have names'

        exp_actions = collections.defaultdict(list)
        exp_values = {}
        exp_names = {}

        if name is not None:
            exp_actions['SET'].append('#name = :name')
            exp_names['#name'] = 'name'
            exp_values[':name'] = name

        if description is not None:
            # empty string deletes
            if description == '':
                exp_actions['REMOVE'].append('description')
            else:
                exp_actions['SET'].append('description = :description')
                exp_values[':description'] = description

        update_query_kwargs = {
            'Key': self.pk(album_id),
            'UpdateExpression': ' '.join([f'{k} {", ".join(v)}' for k, v in exp_actions.items()]),
        }
        if exp_names:
            update_query_kwargs['ExpressionAttributeNames'] = exp_names
        if exp_values:
            update_query_kwargs['ExpressionAttributeValues'] = exp_values
        return self.client.update_item(update_query_kwargs)

    def set_album_art_hash(self, album_id, art_hash):
        update_query_kwargs = {
            'Key': self.pk(album_id),
        }

        if art_hash:
            update_query_kwargs['UpdateExpression'] = 'SET artHash = :ah'
            update_query_kwargs['ExpressionAttributeValues'] = {':ah': art_hash}
        else:
            update_query_kwargs['UpdateExpression'] = 'REMOVE artHash'

        return self.client.update_item(update_query_kwargs)

    def set_delete_at(self, album_id, delete_at):
        "Best effort, logs WARNING on failure"
        query_kwargs = {
            'Key': self.pk(album_id),
            'UpdateExpression': 'SET gsiK1PartitionKey = :pk, gsiK1SortKey = :sk',
            'ExpressionAttributeValues': {':pk': 'album', ':sk': delete_at.to_iso8601_string(), ':zero': 0},
            'ConditionExpression': 'NOT postCount > :zero',
        }
        return self.client.update_item(
            query_kwargs, failure_warning=f'Failed to set deleteAt GSI for album `{album_id}`'
        )

    def clear_delete_at(self, album_id):
        "Best effort, logs WARNING on failure"
        query_kwargs = {
            'Key': self.pk(album_id),
            'UpdateExpression': 'REMOVE gsiK1PartitionKey, gsiK1SortKey',
        }
        return self.client.update_item(
            query_kwargs, failure_warning=f'Failed to clear deleteAt GSI for album `{album_id}`'
        )

    def delete_album(self, album_id):
        if item_deleted := self.client.delete_item(self.pk(album_id)):
            return item_deleted
        raise AlbumDoesNotExist(album_id)

    def increment_post_count(self, album_id, now=None):
        now = now or pendulum.now('utc')
        query_kwargs = {
            'Key': self.pk(album_id),
            'UpdateExpression': 'ADD postCount :one SET postsLastUpdatedAt = :now',
            'ExpressionAttributeValues': {':one': 1, ':now': now.to_iso8601_string()},
        }
        return self.client.update_item(
            query_kwargs,
            failure_warning=f'Failed to increment postCount and set postsLastUpdatedAt for album `{album_id}`',
        )

    def decrement_post_count(self, album_id, now=None):
        now = now or pendulum.now('utc')
        query_kwargs = {
            'Key': self.pk(album_id),
            'UpdateExpression': 'ADD postCount :negative_one SET postsLastUpdatedAt = :now',
            'ExpressionAttributeValues': {':negative_one': -1, ':now': now.to_iso8601_string(), ':zero': 0},
            'ConditionExpression': 'postCount > :zero',
        }
        return self.client.update_item(
            query_kwargs,
            failure_warning=f'Failed to decrement postCount and set postsLastUpdatedAt for album `{album_id}`',
        )

    def update_posts_last_updated_at(self, album_id, now=None):
        now = now or pendulum.now('utc')
        query_kwargs = {
            'Key': self.pk(album_id),
            'UpdateExpression': 'SET postsLastUpdatedAt = :now',
            'ExpressionAttributeValues': {':now': now.to_iso8601_string()},
        }
        return self.client.update_item(
            query_kwargs, failure_warning=f'Failed to update postsLastUpdatedAt for album `{album_id}`'
        )

    def increment_rank_count(self, album_id):
        return self.client.increment_count(self.pk(album_id), 'rankCount')

    def generate_by_user(self, user_id):
        query_kwargs = {
            'KeyConditionExpression': Key('gsiA1PartitionKey').eq(f'album/{user_id}'),
            'IndexName': 'GSI-A1',
        }
        return self.client.generate_all_query(query_kwargs)

    def generate_keys_to_delete(self, cutoff_at):
        query_kwargs = {
            'KeyConditionExpression': 'gsiK1PartitionKey = :pk AND gsiK1SortKey < :sk_max',
            'IndexName': 'GSI-K1',
            'ExpressionAttributeValues': {':pk': 'album', ':sk_max': cutoff_at.to_iso8601_string()},
            'ProjectionExpression': 'partitionKey, sortKey',
        }
        return self.client.generate_all_query(query_kwargs)
