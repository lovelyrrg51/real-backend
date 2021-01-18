import logging

import pendulum

from .exceptions import CardAlreadyExists

logger = logging.getLogger()


class CardDynamo:
    def __init__(self, dynamo_client):
        self.client = dynamo_client

    def pk(self, card_id):
        return {
            'partitionKey': f'card/{card_id}',
            'sortKey': '-',
        }

    def get_card(self, card_id, strongly_consistent=False):
        return self.client.get_item(self.pk(card_id), ConsistentRead=strongly_consistent)

    def add_card(
        self,
        card_id,
        user_id,
        title,
        action,
        sub_title=None,
        created_at=None,
        notify_user_at=None,
        post_id=None,
        comment_id=None,
    ):
        created_at = created_at or pendulum.now('utc')
        query_kwargs = {
            'Item': {
                **self.pk(card_id),
                'schemaVersion': 1,
                'gsiA1PartitionKey': f'user/{user_id}',
                'gsiA1SortKey': f'card/{created_at.to_iso8601_string()}',
                'title': title,
                'action': action,
            },
        }
        if sub_title:
            query_kwargs['Item']['subTitle'] = sub_title
        if notify_user_at:
            query_kwargs['Item']['gsiK1PartitionKey'] = 'card'
            query_kwargs['Item']['gsiK1SortKey'] = notify_user_at.to_iso8601_string() + '/' + user_id
        if post_id:
            query_kwargs['Item']['postId'] = post_id
            query_kwargs['Item']['gsiA2PartitionKey'] = f'card/{post_id}'
            query_kwargs['Item']['gsiA2SortKey'] = user_id
        if comment_id:
            query_kwargs['Item']['commentId'] = comment_id
            query_kwargs['Item']['gsiA3PartitionKey'] = f'card/{comment_id}'
            query_kwargs['Item']['gsiA3SortKey'] = '-'
        try:
            return self.client.add_item(query_kwargs)
        except self.client.exceptions.ConditionalCheckFailedException as err:
            raise CardAlreadyExists(card_id) from err

    def update_title(self, card_id, title):
        query_kwargs = {
            'Key': self.pk(card_id),
            'UpdateExpression': 'SET title = :title',
            'ExpressionAttributeValues': {':title': title},
        }
        return self.client.update_item(query_kwargs)

    def delete_card(self, card_id):
        return self.client.delete_item(self.pk(card_id))

    def clear_notify_user_at(self, card_id):
        query_kwargs = {
            'Key': self.pk(card_id),
            'UpdateExpression': 'REMOVE gsiK1PartitionKey, gsiK1SortKey',
        }
        return self.client.update_item(query_kwargs)

    def generate_cards_by_user(self, user_id, pks_only=False):
        query_kwargs = {
            'KeyConditionExpression': 'gsiA1PartitionKey = :pk AND begins_with(gsiA1SortKey, :sk_prefix)',
            'ExpressionAttributeValues': {':pk': f'user/{user_id}', ':sk_prefix': 'card/'},
            'IndexName': 'GSI-A1',
        }
        gen = self.client.generate_all_query(query_kwargs)
        if pks_only:
            gen = ({'partitionKey': item['partitionKey'], 'sortKey': item['sortKey']} for item in gen)
        return gen

    def generate_card_keys_by_post(self, post_id, user_id=None):
        query_kwargs = {
            'KeyConditionExpression': 'gsiA2PartitionKey = :pk',
            'ExpressionAttributeValues': {':pk': f'card/{post_id}'},
            'ProjectionExpression': 'partitionKey, sortKey',
            'IndexName': 'GSI-A2',
        }
        if user_id:
            query_kwargs['KeyConditionExpression'] += ' AND gsiA2SortKey = :sk'
            query_kwargs['ExpressionAttributeValues'][':sk'] = user_id
        return self.client.generate_all_query(query_kwargs)

    def generate_card_keys_by_comment(self, comment_id):
        query_kwargs = {
            'KeyConditionExpression': 'gsiA3PartitionKey = :pk',
            'ExpressionAttributeValues': {':pk': f'card/{comment_id}'},
            'ProjectionExpression': 'partitionKey, sortKey',
            'IndexName': 'GSI-A3',
        }
        return self.client.generate_all_query(query_kwargs)

    def generate_card_ids_by_notify_user_at(self, cutoff_at, only_user_ids=None):
        query_kwargs = {
            'KeyConditionExpression': 'gsiK1PartitionKey = :c AND gsiK1SortKey < :at_trailing',
            'ExpressionAttributeValues': {':c': 'card', ':at_trailing': cutoff_at.to_iso8601_string() + '/~'},
            'IndexName': 'GSI-K1',
        }
        gen = self.client.generate_all_query(query_kwargs)
        # Note dynamo does not let you apply a FilterExpression to the index/key used in a query
        # 'Filter Expression can only contain non-primary key attributes'
        if only_user_ids:
            gen = (item for item in gen if item['gsiK1SortKey'].split('/')[-1] in only_user_ids)
        gen = (item['partitionKey'].split('/')[1] for item in gen)
        return gen
