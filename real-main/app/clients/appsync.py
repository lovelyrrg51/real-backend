import logging
import os

import boto3
import gql.transport.requests
import requests_aws4auth

APPSYNC_GRAPHQL_URL = os.environ.get('APPSYNC_GRAPHQL_URL')

logger = logging.getLogger()


class AppSyncClient:

    service_name = 'appsync'
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }

    def __init__(self, appsync_graphql_url=APPSYNC_GRAPHQL_URL):
        self.appsync_graphql_url = appsync_graphql_url

    def fire_notification(self, user_id, notification_type, **extra):
        mutation = gql.gql(
            f'''
            mutation TriggerNotification ($input: NotificationInput!) {{
                triggerNotification (input: $input) {{
                    userId
                    type
                    {' '.join(extra.keys())}
                }}
            }}
        '''
        )
        input_obj = {
            'userId': user_id,
            'type': notification_type,
            **extra,
        }
        self.send(mutation, {'input': input_obj})

    def send(self, query, variables):
        aws_session = boto3.session.Session()
        creds = aws_session.get_credentials().get_frozen_credentials()
        auth = requests_aws4auth.AWS4Auth(
            creds.access_key,
            creds.secret_key,
            aws_session.region_name,
            self.service_name,
            session_token=creds.token,
        )
        transport = gql.transport.requests.RequestsHTTPTransport(
            url=self.appsync_graphql_url,
            use_json=True,
            headers=self.headers,
            auth=auth,
        )
        resp = transport.execute(query, variables)
        if resp.errors:
            raise Exception(f'Appsync resp error: `{resp.errors}` from query `{query}`, variables `{variables}`')
