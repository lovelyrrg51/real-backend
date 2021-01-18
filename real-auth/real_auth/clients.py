import logging
import os

import boto3

COGNITO_USER_POOL_ID = os.environ.get('COGNITO_USER_POOL_ID')
COGNITO_BACKEND_CLIENT_ID = os.environ.get('COGNITO_USER_POOL_BACKEND_CLIENT_ID')

logger = logging.getLogger()


class CognitoClient:
    def __init__(self, client_id=COGNITO_BACKEND_CLIENT_ID, user_pool_id=COGNITO_USER_POOL_ID):
        assert client_id, "Cognito user pool client id is required"
        assert user_pool_id, "Cognito user pool id is required"

        self.client_id = client_id
        self.user_pool_id = user_pool_id
        self.aws_region = boto3.Session().region_name

        self.user_pool_client = boto3.client('cognito-idp')
        self.identity_pool_client = boto3.client('cognito-identity')

    def is_username_available(self, preferred_username):
        resp = self.user_pool_client.list_users(
            UserPoolId=self.user_pool_id,
            AttributesToGet=[],
            Filter=f'preferred_username = "{preferred_username.lower()}"',
            Limit=1,
        )
        return not bool(resp['Users'])

    def confirm_user(self, user_id, code):
        "Confirm the user. Returns True for success, False for failure"
        try:
            self.user_pool_client.confirm_sign_up(
                ClientId=self.client_id, Username=user_id, ConfirmationCode=code
            )
        except Exception:
            return False
        return True

    def sign_in(self, user_id):
        "Sign in as the user to the user pool and return tokens, or throw an exception"
        resp = self.user_pool_client.admin_initiate_auth(
            UserPoolId=self.user_pool_id,
            ClientId=self.client_id,
            AuthFlow='CUSTOM_AUTH',
            AuthParameters={'USERNAME': user_id},
        )
        return resp['AuthenticationResult']

    def get_identity_pool_credentials(self, user_id, user_pool_id_token):
        """
        Return credentials from the identity pool, or throw an exception.
        As a side effect, the user pool and identity pool entries will be linked
        (if they weren't already).
        """
        resp = self.identity_pool_client.get_credentials_for_identity(
            IdentityId=user_id,
            Logins={f'cognito-idp.{self.aws_region}.amazonaws.com/{self.user_pool_id}': user_pool_id_token},
        )
        return resp['Credentials']
