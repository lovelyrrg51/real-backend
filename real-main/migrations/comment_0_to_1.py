import logging
import os

import boto3
from boto3.dynamodb.conditions import Key

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

logger = logging.getLogger()


class Migration:
    "Calculate and fill in viewedByCount"

    from_schema_version = 0
    to_schema_version = 1

    def __init__(self, dynamo_table):
        self.dynamo_table = dynamo_table

    def run(self):
        for item in self.generate_all_comments_to_migrate():
            self.migrate_comment(item)

    def generate_all_comments_to_migrate(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': (
                'begins_with(partitionKey, :pk_prefix) AND sortKey = :sk AND schemaVersion = :sv'
            ),
            'ExpressionAttributeValues': {':pk_prefix': 'comment/', ':sk': '-', ':sv': 0},
        }
        while True:
            paginated = self.dynamo_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_comment(self, item):
        pk = item['partitionKey']
        org_count = item.get('viewedByCount')

        # count the existing views for that comment
        kwargs = {
            'KeyConditionExpression': Key('partitionKey').eq(pk) & Key('sortKey').begins_with('view/'),
            'Select': 'COUNT',
        }
        resp = self.dynamo_table.query(**kwargs)
        if resp.get('LastEvaluatedKey') is not None:
            raise Exception(f'Too many views to count for comment `{pk}`')
        new_count = resp['Count']

        # update the comment item, throw exception if there was a race condition with another comment view
        kwargs = {
            'Key': {'partitionKey': pk, 'sortKey': '-'},
            'UpdateExpression': 'SET schemaVersion = :tsv',
            'ConditionExpression': 'attribute_exists(partitionKey)',
            'ExpressionAttributeValues': {':tsv': self.to_schema_version},
        }
        if new_count == 0:
            kwargs['UpdateExpression'] += ' REMOVE viewedByCount'
        else:
            kwargs['UpdateExpression'] += ', viewedByCount = :new_vbc'
            kwargs['ExpressionAttributeValues'][':new_vbc'] = new_count
        if org_count is None:
            kwargs['ConditionExpression'] += ' AND attribute_not_exists(viewedByCount)'
        else:
            kwargs['ConditionExpression'] += ' AND viewedByCount = :org_vbc'
            kwargs['ExpressionAttributeValues'][':org_vbc'] = org_count

        logger.warning(f'Comment `{pk}`: updating viewedByCount from `{org_count}` to `{new_count}`')
        self.dynamo_table.update_item(**kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'
    dynamo_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)

    migration = Migration(dynamo_table)
    migration.run()
