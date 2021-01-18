import base64
import json
import logging
import os
import re

import boto3

DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')
logger = logging.getLogger()


class DynamoClient:
    def __init__(self, table_name=DYNAMO_TABLE, create_table_schema=None):
        """
        If create_table_schema is not None, then the table will be created
        on-the-fly. Useful when testing with a mocked dynamodb backend.
        """
        assert table_name, "Table name is required"
        self.table_name = table_name

        boto3_resource = boto3.resource('dynamodb')
        self.table = (
            boto3_resource.create_table(TableName=table_name, **create_table_schema)
            if create_table_schema
            else boto3_resource.Table(table_name)
        )

        self.boto3_client = boto3.client('dynamodb')
        self.exceptions = self.boto3_client.exceptions

    def add_item(self, query_kwargs):
        "Put an item and return what was putted"
        # ensure query fails if the item already exists
        cond_exp = 'attribute_not_exists(partitionKey)'
        if 'ConditionExpression' in query_kwargs:
            cond_exp += ' and (' + query_kwargs['ConditionExpression'] + ')'
        query_kwargs['ConditionExpression'] = cond_exp
        self.table.put_item(**query_kwargs)
        return query_kwargs.get('Item')

    def get_item(self, pk, **kwargs):
        "Get an item by its primary key"
        return self.table.get_item(Key=pk, **kwargs).get('Item')

    def get_typed_item(self, typed_pk, **kwargs):
        "Get an typed version of the item by its typed primary key"
        return self.boto3_client.get_item(Key=typed_pk, TableName=self.table_name, **kwargs).get('Item')

    def batch_get_items(self, typed_keys, projection_expression=None):
        """
        Get a bunch of items in one batch request.
        Both the input `typed_keys` and the return value should/will be in
        verbose format, with types.
        Order *not* maintained.
        """
        assert len(typed_keys) <= 100, "Max 100 items per batch get request"
        if len(typed_keys) == 0:
            return []
        kwargs = {'RequestItems': {self.table_name: {'Keys': typed_keys}}}
        if projection_expression:
            kwargs['RequestItems'][self.table_name]['ProjectionExpression'] = projection_expression
        return self.boto3_client.batch_get_item(**kwargs)['Responses'][self.table_name]

    def update_item(self, query_kwargs, failure_warning=None):
        """
        Update an item and return the new item.
        Set `failure_warning` fail softly with a logged warning rather than raise an exception.
        """
        # ensure query fails if the item does not exist
        cond_exp = 'attribute_exists(partitionKey)'
        if 'ConditionExpression' in query_kwargs:
            cond_exp += ' and (' + query_kwargs['ConditionExpression'] + ')'
        query_kwargs['ConditionExpression'] = cond_exp
        query_kwargs['ReturnValues'] = 'ALL_NEW'
        try:
            return self.table.update_item(**query_kwargs).get('Attributes')
        except self.exceptions.ConditionalCheckFailedException:
            if failure_warning is None:
                raise
            logger.warning(failure_warning)

    def set_attributes(self, key, **attributes):
        """
        Set the given attributes for the given key.
        If the item does not exist, create it.
        """
        assert attributes, 'Must provide at least one attribute to set'
        kwargs = {
            'Key': key,
            'UpdateExpression': 'SET ' + ', '.join([f'{k} = :{k}' for k in attributes.keys()]),
            'ExpressionAttributeValues': {f':{k}': v for k, v in attributes.items()},
            'ReturnValues': 'ALL_NEW',
        }
        return self.table.update_item(**kwargs).get('Attributes')

    def increment_count(self, key, attribute_name):
        "Best-effort attempt to increment a counter. Logs a WARNING upon failure."
        query_kwargs = {
            'Key': key,
            'UpdateExpression': 'ADD #attrName :one',
            'ExpressionAttributeNames': {'#attrName': attribute_name},
            'ExpressionAttributeValues': {':one': 1},
            'ConditionExpression': 'attribute_exists(partitionKey)',
        }
        failure_warning = f'Failed to increment {attribute_name} for key `{key}`'
        return self.update_item(query_kwargs, failure_warning=failure_warning)

    def decrement_count(self, key, attribute_name):
        "Best-effort attempt to decrement a counter. Logs a WARNING upon failure."
        query_kwargs = {
            'Key': key,
            'UpdateExpression': 'ADD #attrName :neg_one',
            'ExpressionAttributeNames': {'#attrName': attribute_name},
            'ExpressionAttributeValues': {':neg_one': -1, ':zero': 0},
            'ConditionExpression': 'attribute_exists(partitionKey) AND #attrName > :zero',
        }
        failure_warning = f'Failed to decrement {attribute_name} for key `{key}`'
        return self.update_item(query_kwargs, failure_warning=failure_warning)

    def batch_put_items(self, generator):
        "Batch put the items yielded by `generator`. Returns count of how many puts requested."
        cnt = 0
        with self.table.batch_writer() as batch:
            for item in generator:
                batch.put_item(Item=item)
                cnt += 1
        return cnt

    def delete_item(self, pk, **kwargs):
        "Delete an item and return what was deleted"
        return_values = kwargs.pop('ReturnValues', 'ALL_OLD')
        # return None if nothing was deleted, rather than an empty dict
        return self.table.delete_item(Key=pk, ReturnValues=return_values, **kwargs).get('Attributes') or None

    def batch_delete_items(self, generator):
        "Batch delete the items or keys yielded by `generator`. Returns count of how many deletes requested."
        key_generator = ({k: item[k] for k in ('partitionKey', 'sortKey')} for item in generator)
        return self.batch_delete(key_generator)

    def batch_delete(self, key_generator):
        "Batch delete items by keys yielded by `generator`. Returns count of how many deletes requested."
        cnt = 0
        with self.table.batch_writer() as batch:
            for key in key_generator:
                batch.delete_item(Key=key)
                cnt += 1
        return cnt

    def encode_pagination_token(self, last_evaluated_key):
        "From a LastEvaluatedKey to a obfucated string"
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Pagination
        return base64.b64encode(json.dumps(last_evaluated_key).encode('ascii')).decode('utf-8')

    def decode_pagination_token(self, token):
        "From a obfucated string to a ExclusiveStartKey"
        return json.loads(base64.b64decode(token.encode('ascii')).decode('utf-8'))

    def query(self, query_kwargs, limit=None, next_token=None):
        "Query the table and return items & pagination token from the result"
        if limit:
            query_kwargs['Limit'] = limit
        if next_token:
            query_kwargs['ExclusiveStartKey'] = self.decode_pagination_token(next_token)
        resp = self.table.query(**query_kwargs)
        last_key = resp.get('LastEvaluatedKey')
        return {
            'items': resp['Items'],
            'nextToken': self.encode_pagination_token(last_key) if last_key else None,
        }

    def query_head(self, query_kwargs):
        "Query the table and return the first item or None. Does not play well with Filters"
        # Note that supporting a filter expression is possible, but requires a separate codepath
        # if you want to avoid causing negative performance impacts for the common case
        assert 'FilterExpression' not in query_kwargs
        query_kwargs['Limit'] = 1
        resp = self.table.query(**query_kwargs)
        return resp['Items'][0] if resp['Items'] else None

    def generate_all_query(self, query_kwargs):
        "Return a generator that iterates over all results of the query"
        last_key = False
        while last_key is not None:
            start_kwargs = {'ExclusiveStartKey': last_key} if last_key else {}
            resp = self.table.query(**query_kwargs, **start_kwargs)
            for item in resp['Items']:
                yield item
            last_key = resp.get('LastEvaluatedKey')

    def generate_all_scan(self, scan_kwargs):
        "Return a generator that iterates over all results of the scan"
        last_key = False
        while last_key is not None:
            start_kwargs = {'ExclusiveStartKey': last_key} if last_key else {}
            resp = self.table.scan(**scan_kwargs, **start_kwargs)
            for item in resp['Items']:
                yield item
            last_key = resp.get('LastEvaluatedKey')

    def transact_write_items(self, transact_items, transact_exceptions=None):
        """
        Apply the given write operations in a transaction.
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.transact_write_items
        Note that:
            - since this uses the boto dynamo client, rather than the resource, the writes format is more verbose
            - caller does not need to specify TableName

        If one of the transact_item's conditional expressions fails, then the corresponding entry in
        trasact_exceptions will be raised, if it was provided.
        """
        if transact_exceptions is None:
            transact_exceptions = [None] * len(transact_items)
        else:
            assert len(transact_items) == len(transact_exceptions)

        for ti in transact_items:
            list(ti.values()).pop()['TableName'] = self.table_name

        try:
            self.boto3_client.transact_write_items(TransactItems=transact_items)
        except self.boto3_client.exceptions.TransactionCanceledException as err:
            # we want to raise a more specific error than 'the whole transaction failed'
            # there is no way to get the CancellationReasons in boto3, so this is the best we can do
            # https://github.com/aws/aws-sdk-go/issues/2318#issuecomment-443039745
            reasons = re.search(r'\[(.*)\]$', err.response['Error']['Message']).group(1).split(', ')
            for reason, transact_exception in zip(reasons, transact_exceptions):
                if reason == 'ConditionalCheckFailed':
                    # the transact_item with this transaction_exception failed
                    if transact_exception is not None:
                        raise transact_exception from err
            raise err
