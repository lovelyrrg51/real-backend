import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Drop Media.mediaStatus"

    def __init__(self, dynamo_table):
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_all_media_objects_with_media_statuses():
            self.migrate_media_object(item)

    def generate_all_media_objects_with_media_statuses(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND attribute_exists(mediaStatus)',
            'ExpressionAttributeValues': {':pk_prefix': 'media/'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_media_object(self, item):
        media_id = item['mediaId']
        logger.warning(f'Migrating media `{media_id}`')
        query_kwargs = {
            'Key': {'partitionKey': f'media/{media_id}', 'sortKey': '-'},
            'UpdateExpression': 'REMOVE mediaStatus, gsiA2PartitionKey, gsiA2SortKey SET gsiA1SortKey = :sk',
            'ExpressionAttributeValues': {':sk': '-'},
            'ConditionExpression': 'attribute_exists(mediaStatus)',
        }
        self.dynamo_table.update_item(**query_kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'

    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    migration = Migration(dynamo_table)
    migration.run()
