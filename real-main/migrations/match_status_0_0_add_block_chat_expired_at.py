import json
import logging
import os

import boto3
import pendulum

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_MATCHES_TABLE')


class Migration:
    "Add blockChatExpiredAt to dating matches table"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for pv in self.generate_all_views():
            self.add_block_chat_expired_at(pv)

    def generate_all_views(self):
        "Return a generator of all items in the table that pass the filter"
        scan_kwargs = {
            'FilterExpression': 'attribute_exists(userId)',
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def add_block_chat_expired_at(self, pv):
        user_id = pv['userId']
        match_user_id = pv['matchUserId']

        if pv.get('blockChatExpiredAt', None):
            return

        blockChatExpiredAt = pendulum.now('utc') + pendulum.duration(days=30)
        kwargs = {
            'Key': {k: pv[k] for k in ('userId', 'matchUserId')},
            'UpdateExpression': 'SET #tvc = :tvc',
            'ConditionExpression': 'attribute_exists(userId)',
            'ExpressionAttributeNames': {
                '#tvc': 'blockChatExpiredAt',
            },
            'ExpressionAttributeValues': {
                ':tvc': blockChatExpiredAt.to_iso8601_string(),
            },
        }
        logger.warning(f'Adding blockChatExpiredAt `{user_id}` and `{match_user_id}`')
        self.dynamo_table.update_item(**kwargs)


def lambda_handler(event, context):
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    dynamo_client = boto3.client('dynamodb')

    migration = Migration(dynamo_client, dynamo_table)
    migration.run()

    return {'statusCode': 200, 'body': json.dumps('Migration completed successfully')}


if __name__ == '__main__':
    lambda_handler(None, None)
