from unittest import mock

import boto3
import moto
import pytest

from .table_schema import feed_table_schema, main_table_schema


@pytest.fixture
def dynamo_client_and_tables():
    """
    Both the dynamo_client and dynamo_table must be generated under the same mock_dynamodb2
    instance in order for catching of exceptions thrown by operations with the table
    using the error definitions on the client to work.
    """
    with moto.mock_dynamodb2():
        client = boto3.client('dynamodb')
        table = boto3.resource('dynamodb').create_table(
            TableName='test-main-table', BillingMode='PAY_PER_REQUEST', **main_table_schema
        )
        feed_table = boto3.resource('dynamodb').create_table(
            TableName='test-feed-table', BillingMode='PAY_PER_REQUEST', **feed_table_schema
        )
        # use if table already exists (when running directly against dynamo)
        # table = boto3.resource('dynamodb').Table('test-main-table')
        yield client, table, feed_table


@pytest.fixture
def dynamo_client(dynamo_client_and_tables):
    yield dynamo_client_and_tables[0]


@pytest.fixture
def dynamo_table(dynamo_client_and_tables):
    yield dynamo_client_and_tables[1]


@pytest.fixture
def dynamo_feed_table(dynamo_client_and_tables):
    yield dynamo_client_and_tables[2]


@pytest.fixture
def s3_bucket():
    with moto.mock_s3():
        yield boto3.resource('s3').create_bucket(Bucket='test-bucket')


@pytest.fixture
def pinpoint_client():
    yield mock.Mock(boto3.client('pinpoint'))
