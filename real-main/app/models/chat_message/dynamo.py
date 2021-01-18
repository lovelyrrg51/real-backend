import logging

from boto3.dynamodb.conditions import Key

logger = logging.getLogger()


class ChatMessageDynamo:
    def __init__(self, dynamo_client):
        self.client = dynamo_client

    def pk(self, message_id):
        return {
            'partitionKey': f'chatMessage/{message_id}',
            'sortKey': '-',
        }

    def get_chat_message(self, message_id, strongly_consistent=False):
        return self.client.get_item(self.pk(message_id), ConsistentRead=strongly_consistent)

    def add_chat_message(self, message_id, chat_id, author_user_id, text, text_tags, now):
        created_at_str = now.to_iso8601_string()
        query_kwargs = {
            'Item': {
                'schemaVersion': 1,
                'partitionKey': f'chatMessage/{message_id}',
                'sortKey': '-',
                'gsiA1PartitionKey': f'chatMessage/{chat_id}',
                'gsiA1SortKey': created_at_str,
                'messageId': message_id,
                'chatId': chat_id,
                'createdAt': created_at_str,
                'text': text,
                'textTags': text_tags,
            },
            'ConditionExpression': 'attribute_not_exists(partitionKey)',  # no updates, just adds
        }
        if author_user_id:
            query_kwargs['Item']['userId'] = author_user_id
        return self.client.add_item(query_kwargs)

    def edit_chat_message(self, message_id, text, text_tags, now):
        query_kwargs = {
            'Key': self.pk(message_id),
            'UpdateExpression': 'SET lastEditedAt = :at, #textName = :text, textTags = :textTags',
            'ExpressionAttributeNames': {'#textName': 'text'},
            'ExpressionAttributeValues': {':at': now.to_iso8601_string(), ':text': text, ':textTags': text_tags},
            'ConditionExpression': 'attribute_exists(partitionKey)',
        }
        return self.client.update_item(query_kwargs)

    def increment_flag_count(self, message_id):
        return self.client.increment_count(self.pk(message_id), 'flagCount')

    def decrement_flag_count(self, message_id):
        return self.client.decrement_count(self.pk(message_id), 'flagCount')

    def delete_chat_message(self, message_id):
        return self.client.delete_item(self.pk(message_id))

    def generate_chat_messages_by_chat(self, chat_id, pks_only=False):
        query_kwargs = {
            'KeyConditionExpression': Key('gsiA1PartitionKey').eq(f'chatMessage/{chat_id}'),
            'IndexName': 'GSI-A1',
        }
        gen = self.client.generate_all_query(query_kwargs)
        if pks_only:
            gen = ({'partitionKey': item['partitionKey'], 'sortKey': item['sortKey']} for item in gen)
        return gen

    def generate_all_chat_messages_by_scan(self):
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND sortKey = :sk_prefix',
            'ExpressionAttributeValues': {':pk_prefix': 'chatMessage/', ':sk_prefix': '-'},
        }
        return self.client.generate_all_scan(scan_kwargs)
