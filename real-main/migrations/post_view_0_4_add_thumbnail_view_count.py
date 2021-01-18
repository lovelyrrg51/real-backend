import json
import logging
import os

import boto3

logger = logging.getLogger()

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


class Migration:
    "Add thumbnailViewCount to post views"

    def __init__(self, dynamo_client, dynamo_table):
        self.dynamo_client = dynamo_client
        self.dynamo_table = dynamo_table

    def run(self):
        for pv in self.generate_all_post_views():
            self.migrate_post_view(pv)

    def generate_all_post_views(self):
        "Return a generator of all items in the table that pass the filter"
        scan_kwargs = {
            'FilterExpression': ' AND '.join(
                [
                    'begins_with(partitionKey, :pk_prefix)',
                    'begins_with(sortKey, :sk_prefix)',
                ]
            ),
            'ExpressionAttributeValues': {':pk_prefix': 'post/', ':sk_prefix': 'view/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_post_view(self, pv):
        post_id = pv['partitionKey'].split('/')[1]
        user_id = pv['sortKey'].split('/')[1]
        view_count = pv.get('viewCount', 0)
        focus_view_count = pv.get('focusViewCount')
        thumbnail_view_count = view_count - (focus_view_count or 0)

        if pv.get('thumbnailViewCount', 0) == thumbnail_view_count:
            return

        kwargs = {
            'Key': {k: pv[k] for k in ('partitionKey', 'sortKey')},
            'UpdateExpression': 'SET #tvc = :tvc',
            'ConditionExpression': ' AND '.join(
                [
                    'attribute_exists(partitionKey)',
                    '#vc = :vc',
                ]
            ),
            'ExpressionAttributeNames': {
                '#vc': 'viewCount',
                '#tvc': 'thumbnailViewCount',
            },
            'ExpressionAttributeValues': {
                ':vc': view_count,
                ':tvc': thumbnail_view_count,
            },
        }
        if focus_view_count is not None:
            kwargs['ConditionExpression'] += ' AND #fvc = :fvc'
            kwargs['ExpressionAttributeNames']['#fvc'] = 'focusViewCount'
            kwargs['ExpressionAttributeValues'][':fvc'] = focus_view_count
        logger.warning(f'Migrating post view item post `{post_id}` and user `{user_id}`')
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
