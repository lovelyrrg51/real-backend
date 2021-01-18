import boto3
import botocore


class S3Client:
    def __init__(self, bucket_name, create_bucket=False):
        """
        The create_bucket kwarg is intended for use with moto in the test suite.
        """
        assert bucket_name, "Bucket name is required"
        self.boto_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket_name)
        self.exceptions = self.boto_client.exceptions

        if create_bucket:
            self.s3.create_bucket(Bucket=bucket_name)

    def get_object_data_stream(self, path):
        return self.bucket.Object(path).get()['Body']

    def get_object_checksum(self, path):
        resp = self.boto_client.head_object(Bucket=self.bucket_name, Key=path)
        # etags start and end with '"', as required by RFC
        checksum = resp['ResponseMetadata']['HTTPHeaders']['etag'][1:-1]
        # Not all S3 etags are md5 checksums
        # https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
        if '-' in checksum:
            raise ValueError(f'S3 object at `{path}` does not have md5 etag')
        return checksum

    def list_common_prefixes(self, path_prefix):
        resp = self.boto_client.list_objects_v2(Bucket=self.bucket_name, Delimiter='/', Prefix=path_prefix)
        return [cp['Prefix'] for cp in resp.get('CommonPrefixes', [])]

    def delete_object(self, path):
        self.bucket.Object(path).delete()

    def delete_objects(self, paths):
        "Delete mutliple objects in one call to S3"
        kwargs = {'Delete': {'Objects': [{'Key': p} for p in paths]}}
        self.bucket.delete_objects(**kwargs)

    def delete_objects_with_prefix(self, path_prefix):
        "Delete mutliple objects with the same prefix in one call to S3"
        self.bucket.objects.filter(Prefix=path_prefix).delete()

    def copy_object(self, old_path, new_path):
        new_obj = self.bucket.Object(new_path)
        new_obj.copy({'Bucket': self.bucket.name, 'Key': old_path})

    def put_object(self, path, body, content_type):
        self.bucket.put_object(Key=path, Body=body, ContentType=content_type)

    def exists(self, path):
        # https://stackoverflow.com/a/33843019
        try:
            self.s3.Object(self.bucket_name, path).load()
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == "404":
                return False
            raise
        return True
