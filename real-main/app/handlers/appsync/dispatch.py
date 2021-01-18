"AppSync GraphQL data source"
import logging
import os

from app.logging import LogLevelContext, handler_logging

from . import routes
from .exceptions import ClientException

logger = logging.getLogger()

# use this in the test suite to turn off auto-disocvery of routes
route_path = os.environ.get('APPSYNC_ROUTE_AUTODISCOVERY_PATH', 'app.handlers.appsync.handlers')
if route_path:
    routes.discover(route_path)


def get_client_details(event):
    headers = event['request']['headers']
    client = {
        'version': headers.get('x-real-version'),
        'device': headers.get('x-real-device'),
        'system': headers.get('x-real-system'),
        'uid': headers.get('x-real-uid'),
    }
    return {k: v for k, v in client.items() if v is not None}


def get_gql_details(event):
    return {
        'arguments': event.get('arguments') or {},
        'field': event['info']['parentTypeName'] + '.' + event['info']['fieldName'],
        'source': event.get('source') or {},
        'callerUserId': (event.get('identity') or {}).get('cognitoIdentityId'),
    }


def event_to_extras(event):
    client = get_client_details(event)
    gql = get_gql_details(event)
    return {'gql': gql, 'client': client}


@handler_logging(event_to_extras=event_to_extras)
def dispatch(event, context):
    "Top-level dispatch of appsync event to the correct handler"
    # it is a sin that python has no dictionary destructing asignment
    client = get_client_details(event)
    gql = get_gql_details(event)

    field = gql['field']
    handler = routes.get_handler(field)
    if not handler:
        # should not be able to get here
        msg = f'No handler for field `{field}` found'
        logger.exception(msg)
        raise Exception(msg)

    # we suppress INFO logging, except this message
    with LogLevelContext(logger, logging.INFO):
        logger.info(f'Handling AppSync GQL resolution of `{field}`')

    try:
        data = handler(
            gql['callerUserId'],
            gql['arguments'],
            source=gql['source'],
            context=context,
            event=event,
            client=client,
        )
    except ClientException as err:
        logger.warning(str(err))
        return {'error': err.serialize()}

    return {'data': data}
