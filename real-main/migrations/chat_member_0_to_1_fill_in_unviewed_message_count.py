import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "Remove post trendings for non-verified and non-original posts"

    from_version = 0
    to_version = 1

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_chat_member_items():
            chat_id = item['partitionKey'][len('chat/') :]
            user_id = item['sortKey'][len('member/') :]
            prev_cnt = item.get('unviewedMessageCount')
            new_cnt = self.get_unviewed_message_count(chat_id, user_id)
            self.set_unviewed_message_count(chat_id, user_id, prev_cnt, new_cnt)

    def generate_chat_member_items(self):
        scan_kwargs = {
            'FilterExpression': ' AND '.join(
                [
                    'begins_with(partitionKey, :pk_prefix)',
                    'begins_with(sortKey, :sk_prefix)',
                    'schemaVersion = :fsv',
                ]
            ),
            'ExpressionAttributeValues': {
                ':pk_prefix': 'chat/',
                ':sk_prefix': 'member/',
                ':fsv': self.from_version,
            },
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def get_unviewed_message_count(self, chat_id, user_id):
        total_cnt = 0
        viewed_cnt = 0
        for message in self.generate_chat_messages(chat_id):
            total_cnt += 1
            if message['userId'] == user_id:
                viewed_cnt += 1
            elif self.get_message_view(message['messageId'], user_id):
                viewed_cnt += 1
        return total_cnt - viewed_cnt

    def generate_chat_messages(self, chat_id):
        query_kwargs = {
            'KeyConditionExpression': 'gsiA1PartitionKey = :gsia1pk',
            'ExpressionAttributeValues': {':gsia1pk': f'chatMessage/{chat_id}'},
            'IndexName': 'GSI-A1',
        }
        while True:
            paginated = self.dynamo_table.query(**query_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            query_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def get_message_view(self, message_id, user_id):
        key = {'partitionKey': f'chatMessage/{message_id}', 'sortKey': f'view/{user_id}'}
        return self.dynamo_table.get_item(Key=key).get('Item')

    def set_unviewed_message_count(self, chat_id, user_id, prev_cnt, new_cnt):
        query_kwargs = {
            'Key': {'partitionKey': f'chat/{chat_id}', 'sortKey': f'member/{user_id}'},
            'UpdateExpression': 'SET schemaVersion = :tsv',
            'ConditionExpression': 'schemaVersion = :fsv',
            'ExpressionAttributeValues': {':tsv': self.to_version, ':fsv': self.from_version},
        }
        if prev_cnt is None:
            query_kwargs['ConditionExpression'] += ' AND attribute_not_exists(unviewedMessageCount)'
        else:
            query_kwargs['ConditionExpression'] += ' AND unviewedMessageCount = :pumc'
            query_kwargs['ExpressionAttributeValues'][':pumc'] = prev_cnt
        if new_cnt > 0:
            query_kwargs['UpdateExpression'] += ', unviewedMessageCount = :numc'
            query_kwargs['ExpressionAttributeValues'][':numc'] = new_cnt
        logger.warning(f'Member chat `{chat_id}`, user `{user_id}`: setting unviewedMessageCount to `{new_cnt}`')
        return self.dynamo_table.update_item(**query_kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_client = boto3.client('dynamodb')
    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
