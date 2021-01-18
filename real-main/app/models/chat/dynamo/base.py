import logging

import pendulum
from boto3.dynamodb.conditions import Key

from ..enums import ChatType

logger = logging.getLogger()


class ChatDynamo:
    def __init__(self, dynamo_client):
        self.client = dynamo_client

    def pk(self, chat_id):
        return {
            'partitionKey': f'chat/{chat_id}',
            'sortKey': '-',
        }

    def typed_pk(self, chat_id):
        return {
            'partitionKey': {'S': f'chat/{chat_id}'},
            'sortKey': {'S': '-'},
        }

    def get(self, chat_id, strongly_consistent=False):
        return self.client.get_item(self.pk(chat_id), ConsistentRead=strongly_consistent)

    def get_direct_chat(self, user_id_1, user_id_2):
        user_ids = sorted([user_id_1, user_id_2])
        query_kwargs = {
            'KeyConditionExpression': Key('gsiA1PartitionKey').eq(f'chat/{user_ids[0]}/{user_ids[1]}'),
            'IndexName': 'GSI-A1',
        }
        return self.client.query_head(query_kwargs)

    def transact_add(self, chat_id, chat_type, created_by_user_id, with_user_id=None, name=None, now=None):
        # with_user_id parameter is required for direct chats, forbidden for group
        if chat_type == ChatType.DIRECT:
            assert with_user_id, 'DIRECT chats require with_user_id kwarg'
        if chat_type == ChatType.GROUP:
            assert with_user_id is None, 'GROUP chat forbit with_user_id kwarg'

        now = now or pendulum.now('utc')
        created_at_str = now.to_iso8601_string()
        query_kwargs = {
            'Put': {
                'Item': {
                    'schemaVersion': {'N': '0'},
                    'partitionKey': {'S': f'chat/{chat_id}'},
                    'sortKey': {'S': '-'},
                    'chatId': {'S': chat_id},
                    'chatType': {'S': chat_type},
                    'createdAt': {'S': created_at_str},
                    'createdByUserId': {'S': created_by_user_id},
                },
                'ConditionExpression': 'attribute_not_exists(partitionKey)',  # no updates, just adds
            }
        }
        if name:
            query_kwargs['Put']['Item']['name'] = {'S': name}
        if with_user_id:
            user_id_1, user_id_2 = sorted([created_by_user_id, with_user_id])
            query_kwargs['Put']['Item']['userCount'] = {'N': '2'}
            query_kwargs['Put']['Item']['gsiA1PartitionKey'] = {'S': f'chat/{user_id_1}/{user_id_2}'}
            query_kwargs['Put']['Item']['gsiA1SortKey'] = {'S': '-'}
        else:
            query_kwargs['Put']['Item']['userCount'] = {'N': '1'}
        return query_kwargs

    def update_name(self, chat_id, name):
        "Set `name` to empty string to delete"
        query_kwargs = {
            'Key': self.pk(chat_id),
            'ExpressionAttributeNames': {'#name': 'name'},
        }
        if name:
            query_kwargs['UpdateExpression'] = 'SET #name = :name'
            query_kwargs['ExpressionAttributeValues'] = {':name': name}
        else:
            query_kwargs['UpdateExpression'] = 'REMOVE #name'
        return self.client.update_item(query_kwargs)

    def update_last_message_activity_at(self, chat_id, now):
        now_str = now.to_iso8601_string()
        query_kwargs = {
            'Key': self.pk(chat_id),
            'UpdateExpression': 'SET lastMessageActivityAt = :at',
            'ExpressionAttributeValues': {':at': now_str},
            'ConditionExpression': 'attribute_exists(partitionKey) AND NOT :at < lastMessageActivityAt',
        }
        msg = f'Failed to update last message activity for chat `{chat_id}` to `{now_str}`'
        return self.client.update_item(query_kwargs, failure_warning=msg)

    def increment_flag_count(self, chat_id):
        return self.client.increment_count(self.pk(chat_id), 'flagCount')

    def decrement_flag_count(self, chat_id):
        return self.client.decrement_count(self.pk(chat_id), 'flagCount')

    def increment_messages_count(self, chat_id):
        return self.client.increment_count(self.pk(chat_id), 'messagesCount')

    def decrement_messages_count(self, chat_id):
        return self.client.decrement_count(self.pk(chat_id), 'messagesCount')

    def delete(self, chat_id):
        return self.client.delete_item(self.pk(chat_id))

    def transact_increment_user_count(self, chat_id):
        return {
            'Update': {
                'Key': self.typed_pk(chat_id),
                'UpdateExpression': 'ADD userCount :one',
                'ExpressionAttributeValues': {':one': {'N': '1'}},
                'ConditionExpression': 'attribute_exists(partitionKey)',
            }
        }

    def transact_decrement_user_count(self, chat_id):
        return {
            'Update': {
                'Key': self.typed_pk(chat_id),
                'UpdateExpression': 'ADD userCount :negOne',
                'ExpressionAttributeValues': {':negOne': {'N': '-1'}, ':zero': {'N': '0'}},
                'ConditionExpression': 'attribute_exists(partitionKey) AND userCount > :zero',
            }
        }
