import json
import logging

from .exceptions import ClientException
from .logging import LogLevelContext, logger


def handler(required_query_params=None):
    "Decorator to simplify handlers"
    required_query_params = required_query_params or []

    def decorator(func):
        def inner(event, context):
            extra_args = []
            query_string_params = event.get('queryStringParameters') or {}
            for qp in required_query_params:
                if qp not in query_string_params:
                    raise ClientException(f'Query parameter `{qp}` is required')
                extra_args.append(query_string_params[qp])
            return func(event, context, *extra_args)

        def outer(event, context):
            with LogLevelContext(logger, logging.INFO):
                logger.info(f'Handling `{func.__name__}` event', extra={'event': event})
            try:
                data = inner(event, context)
            except ClientException as err:
                return {'statusCode': 400, 'body': json.dumps({'message': str(err)}, default=str)}
            return {
                'statusCode': 200,
                'body': json.dumps(data, default=str),
            }

        return outer

    return decorator
