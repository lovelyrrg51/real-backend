import logging

import pendulum
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()


class ChatMemberDynamo:
    def __init__(self, dynamo_client):
        self.client = dynamo_client

    def pk(self, chat_id, user_id):
        return {
            'partitionKey': f'chat/{chat_id}',
            'sortKey': f'member/{user_id}',
        }

    def typed_pk(self, chat_id, user_id):
        return {
            'partitionKey': {'S': f'chat/{chat_id}'},
            'sortKey': {'S': f'member/{user_id}'},
        }

    def get(self, chat_id, user_id, strongly_consistent=False):
        return self.client.get_item(self.pk(chat_id, user_id), ConsistentRead=strongly_consistent)

    def transact_add(self, chat_id, user_id, now=None):
        now = now or pendulum.now('utc')
        joined_at_str = now.to_iso8601_string()
        return {
            'Put': {
                'Item': {
                    'schemaVersion': {'N': '1'},
                    'partitionKey': {'S': f'chat/{chat_id}'},
                    'sortKey': {'S': f'member/{user_id}'},
                    'gsiK1PartitionKey': {'S': f'chat/{chat_id}'},
                    'gsiK1SortKey': {'S': f'member/{joined_at_str}'},
                    'gsiK2PartitionKey': {'S': f'member/{user_id}'},
                    'gsiK2SortKey': {'S': f'chat/{joined_at_str}'},  # actually tracks lastMessageActivityAt
                },
                'ConditionExpression': 'attribute_not_exists(partitionKey)',  # no updates, just adds
            }
        }

    def transact_delete(self, chat_id, user_id):
        return {
            'Delete': {
                'Key': self.typed_pk(chat_id, user_id),
                'ConditionExpression': 'attribute_exists(partitionKey)',
            }
        }

    def delete(self, chat_id, user_id):
        return self.client.delete_item(self.pk(chat_id, user_id))

    def update_last_message_activity_at(self, chat_id, user_id, now):
        "Best effort to update last message activity at. Logs WARNING on failure."
        now_str = now.to_iso8601_string()
        query_kwargs = {
            'Key': self.pk(chat_id, user_id),
            'UpdateExpression': 'SET gsiK2SortKey = :gsik2sk',
            'ExpressionAttributeValues': {':gsik2sk': 'chat/' + now_str},
            'ConditionExpression': 'attribute_exists(partitionKey) AND NOT :gsik2sk < gsiK2SortKey',
        }
        msg = f'Failed to update last message activity for chat `{chat_id}` and member `{user_id}` to `{now_str}`'
        return self.client.update_item(query_kwargs, failure_warning=msg)

    def increment_messages_unviewed_count(self, chat_id, user_id):
        return self.client.increment_count(self.pk(chat_id, user_id), 'messagesUnviewedCount')

    def decrement_messages_unviewed_count(self, chat_id, user_id):
        return self.client.decrement_count(self.pk(chat_id, user_id), 'messagesUnviewedCount')

    def clear_messages_unviewed_count(self, chat_id, user_id):
        query_kwargs = {
            'Key': self.pk(chat_id, user_id),
            'UpdateExpression': 'REMOVE messagesUnviewedCount',
            'ConditionExpression': 'attribute_exists(partitionKey)',
        }
        return self.client.update_item(query_kwargs)

    def generate_user_ids_by_chat(self, chat_id):
        query_kwargs = {
            'KeyConditionExpression': (
                Key('gsiK1PartitionKey').eq(f'chat/{chat_id}') & Key('gsiK1SortKey').begins_with('member/')
            ),
            'IndexName': 'GSI-K1',
        }
        return map(lambda item: item['sortKey'][len('member/') :], self.client.generate_all_query(query_kwargs))

    def generate_chat_ids_by_user(self, user_id):
        query_kwargs = {
            'KeyConditionExpression': (
                Key('gsiK2PartitionKey').eq(f'member/{user_id}') & Key('gsiK2SortKey').begins_with('chat/')
            ),
            'IndexName': 'GSI-K2',
        }
        return map(
            lambda item: item['partitionKey'][len('chat/') :], self.client.generate_all_query(query_kwargs)
        )
