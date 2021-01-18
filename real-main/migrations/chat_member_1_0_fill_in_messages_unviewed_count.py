import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "Move ChatMember.unviewedMessageCount to ChatMember.messagesUnviewedCount"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_chat_member_items():
            self.migrate_chat_member(item)

    def generate_chat_member_items(self):
        scan_kwargs = {
            'FilterExpression': ' AND '.join(
                [
                    'begins_with(partitionKey, :pk_prefix)',
                    'begins_with(sortKey, :sk_prefix)',
                    'attribute_exists(unviewedMessageCount)',
                ]
            ),
            'ExpressionAttributeValues': {':pk_prefix': 'chat/', ':sk_prefix': 'member/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_chat_member(self, item):
        old_cnt = item['unviewedMessageCount']
        query_kwargs = {
            'Key': {k: item[k] for k in ('partitionKey', 'sortKey')},
            'UpdateExpression': 'ADD messagesUnviewedCount :oc REMOVE unviewedMessageCount',
            'ConditionExpression': 'unviewedMessageCount = :oc',
            'ExpressionAttributeValues': {':oc': old_cnt},
        }
        logger.warning(
            f'Migrating chat member `{item["partitionKey"]}` / `{item["sortKey"]}` with unviewedMessageCount of `{old_cnt}`'
        )
        return self.dynamo_table.update_item(**query_kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_client = boto3.client('dynamodb')
    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()
