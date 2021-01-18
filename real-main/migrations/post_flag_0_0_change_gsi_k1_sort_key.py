import logging
import os

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Change PostFlag.gsiK1SortKey from '-' to 'post'"

    def __init__(self, dynamo_table):
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_all_post_flags_that_need_migrating():
            self.migrate_post_flag(item)

    def generate_all_post_flags_that_need_migrating(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(gsiK1PartitionKey, :pk_prefix) AND gsiK1SortKey = :sk',
            'ExpressionAttributeValues': {':pk_prefix': 'flag/', ':sk': '-'},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_post_flag(self, item):
        pk = {
            'partitionKey': item['partitionKey'],
            'sortKey': item['sortKey'],
        }
        kwargs = {
            'Key': pk,
            'UpdateExpression': 'SET gsiK1SortKey = :new_sk',
            'ConditionExpression': 'attribute_exists(partitionKey)',
            'ExpressionAttributeValues': {':new_sk': 'post'},
        }
        logger.warning(f'Post Flag `{pk}`: migrating')
        self.dynamo_table.update_item(**kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'
    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    migration = Migration(dynamo_table)
    migration.run()
