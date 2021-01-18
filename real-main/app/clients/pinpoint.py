import logging
import os
import uuid

import boto3

PINPOINT_APPLICATION_ID = os.environ.get('PINPOINT_APPLICATION_ID')

logger = logging.getLogger()


class PinpointClient:
    def __init__(self, app_id=PINPOINT_APPLICATION_ID):
        self.app_id = app_id
        self.client = boto3.client('pinpoint')

    def send_user_apns(self, user_id, url, title, body=None):
        "Returns a bool representing if the APNS was successfully sent"
        apns_msg = {'Action': 'URL', 'Title': title, 'Url': url}
        if body:
            apns_msg['Body'] = body
        kwargs = {
            'ApplicationId': self.app_id,
            'SendUsersMessageRequest': {
                'MessageConfiguration': {'APNSMessage': apns_msg},
                'Users': {user_id: {}},
            },
        }
        result = self.client.send_users_messages(**kwargs)['SendUsersMessageResponse']['Result'][user_id]
        return 'SUCCESSFUL' in (v['DeliveryStatus'] for k, v in result.items())

    def update_user_endpoint(self, user_id, channel_type, address):
        """
        Set the user's endpoint of type `channel_type` to `address`.

        The user should have at most one active endpoint of each `channel_type`.
        If this method finds more than one active endpoint for the given
        `channel_type`, it will set `address` on one of them and delete the extras.
        """
        endpoints = self.get_user_endpoints(user_id, channel_type=channel_type)
        endpoint_ids = []
        for this_endpoint_id, this_endpoint in endpoints.items():
            # put the endpoint to keep at the front
            if this_endpoint.get('Address') == address:
                endpoint_ids.insert(0, this_endpoint_id)
            else:
                endpoint_ids.append(this_endpoint_id)

        # delete extras
        while len(endpoint_ids) > 1:
            self.delete_endpoint(endpoint_ids.pop())

        endpoint_id = endpoint_ids[0] if endpoint_ids else str(uuid.uuid4())
        if endpoints.get(endpoint_id) != address:
            kwargs = {
                'ApplicationId': self.app_id,
                'EndpointId': endpoint_id,
                'EndpointRequest': {'Address': address, 'ChannelType': channel_type, 'User': {'UserId': user_id}},
            }
            self.client.update_endpoint(**kwargs)
        return endpoint_id

    def get_user_endpoints(self, user_id, channel_type=None):
        """
        A dict of {endpoint_id: endpoint_details} where endpoint_details is
        a EndpointsResponse.Item as returned by the boto lib
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/pinpoint.html#Pinpoint.Client.get_user_endpoints
        """
        kwargs = {
            'ApplicationId': self.app_id,
            'UserId': user_id,
        }
        try:
            resp = self.client.get_user_endpoints(**kwargs)
        except self.client.exceptions.NotFoundException:
            return {}

        return {
            item['Id']: item
            for item in resp['EndpointsResponse']['Item']
            if not channel_type or item['ChannelType'] == channel_type
        }

    def enable_user_endpoints(self, user_id):
        "Disable all of a user's endpoints"
        endpoints = self.get_user_endpoints(user_id)
        for endpoint_id in endpoints.keys():
            kwargs = {
                'ApplicationId': self.app_id,
                'EndpointId': endpoint_id,
                'EndpointRequest': {'EndpointStatus': 'ACTIVE'},
            }
            self.client.update_endpoint(**kwargs)

    def disable_user_endpoints(self, user_id):
        "Disable all of a user's endpoints"
        endpoints = self.get_user_endpoints(user_id)
        for endpoint_id in endpoints.keys():
            kwargs = {
                'ApplicationId': self.app_id,
                'EndpointId': endpoint_id,
                'EndpointRequest': {'EndpointStatus': 'INACTIVE'},
            }
            self.client.update_endpoint(**kwargs)

    def delete_endpoint(self, endpoint_id):
        "Delete a specific endpoint"
        kwargs = {
            'ApplicationId': self.app_id,
            'EndpointId': endpoint_id,
        }
        self.client.delete_endpoint(**kwargs)

    def delete_user_endpoint(self, user_id, channel_type):
        "Delete a user's endpoint of a specific `channel_type`"
        endpoints = self.get_user_endpoints(user_id, channel_type=channel_type)
        for endpoint_id in endpoints.keys():
            self.delete_endpoint(endpoint_id)

    def delete_user_endpoints(self, user_id):
        "Delete all of a user's endpoints"
        kwargs = {
            'ApplicationId': self.app_id,
            'UserId': user_id,
        }
        self.client.delete_user_endpoints(**kwargs)
