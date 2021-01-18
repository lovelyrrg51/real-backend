import logging
import os

import boto3
import botocore

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
S3_UPLOADS_BUCKET = os.environ.get('S3_UPLOADS_BUCKET')

logger = logging.getLogger()


class _ImageSize:

    file_ext = 'jpg'

    def __init__(self, name, max_dimensions):
        self.name = name
        self.max_dimensions = max_dimensions
        self.filename = f'{self.name}.{self.file_ext}'


NATIVE = _ImageSize('native', None)
K4 = _ImageSize('4K', (3840, 2160))
P1080 = _ImageSize('1080p', (1920, 1080))
P480 = _ImageSize('480p', (854, 480))
P64 = _ImageSize('64p', (114, 64))

ALL_SIZES = (NATIVE, K4, P1080, P480, P64)


class Migration:
    """
    Delete images in S3 for each media object from media-object-specific paths.
    Part of removing support for multiple-image posts.
    """

    from_schema_version = 1
    to_schema_version = 2

    def __init__(self, boto_table, s3_bucket):
        self.boto_table = boto_table
        self.s3_bucket = s3_bucket

    def run(self):
        for item in self.generate_all_pending_media_objects():
            self.migrate_media_object(item)

    def generate_all_pending_media_objects(self):
        "Return a generator of all items that need to be migrated"
        scan_kwargs = {
            'FilterExpression': 'begins_with(partitionKey, :pk_prefix) AND schemaVersion = :fsv',
            'ExpressionAttributeValues': {':pk_prefix': 'media/', ':fsv': self.from_schema_version},
        }
        while True:
            paginated = self.boto_table.scan(**scan_kwargs)
            for item in paginated['Items']:
                yield item
            if 'LastEvaluatedKey' not in paginated:
                break
            scan_kwargs['ExclusiveStartKey'] = paginated['LastEvaluatedKey']

    def migrate_media_object(self, item):
        self.delete_old_s3_objects(item)
        self.dynamo_update_schema_version(item)

    def delete_old_s3_objects(self, item):
        for size in ALL_SIZES:
            old_path = '/'.join([item['userId'], 'post', item['postId'], 'media', item['mediaId'], size.filename])
            if not self.s3_exists(old_path):
                continue

            logger.warning(f'MediaObject `{item["mediaId"]}`: s3 delete `{old_path}`')
            self.s3_bucket.Object(old_path).delete()

    def s3_exists(self, path):
        try:
            self.s3_bucket.Object(path).load()
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == "404":
                return False
            raise
        return True

    def dynamo_update_schema_version(self, item):
        logger.warning(f'MediaObject `{item["mediaId"]}`: updating dynamo schema version')
        kwargs = {
            'Key': {'partitionKey': item['partitionKey'], 'sortKey': item['sortKey']},
            'UpdateExpression': 'SET schemaVersion = :tsv',
            'ExpressionAttributeValues': {':tsv': self.to_schema_version, ':fsv': self.from_schema_version},
            'ConditionExpression': 'attribute_exists(partitionKey) AND schemaVersion = :fsv',
        }
        self.boto_table.update_item(**kwargs)


if __name__ == '__main__':
    assert DYNAMO_TABLE, 'Must set env variable DYNAMO_TABLE to dynamo table name'
    assert S3_UPLOADS_BUCKET, 'Must set env variable S3_UPLOADS_BUCKET to bucket name'

    boto_table = boto3.resource('dynamodb').Table(DYNAMO_TABLE)
    s3_bucket = boto3.resource('s3').Bucket(S3_UPLOADS_BUCKET)

    migration = Migration(boto_table, s3_bucket)
    migration.run()
