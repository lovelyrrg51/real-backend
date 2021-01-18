from .clients import CognitoClient
from .dispatch import handler
from .enums import UsernameStatus
from .exceptions import ClientException
from .validate import validate_username
from .xray import patch_all

patch_all()
cognito_client = CognitoClient()


@handler(required_query_params=['username'])
def get_username_status(event, context, username):
    if not validate_username(username):
        status = UsernameStatus.INVALID
    elif cognito_client.is_username_available(username):
        status = UsernameStatus.AVAILABLE
    else:
        status = UsernameStatus.NOT_AVAILABLE
    return {'status': status}


@handler(required_query_params=['userId', 'code'])
def post_user_confirm(event, context, user_id, code):
    if not cognito_client.confirm_user(user_id, code):
        raise ClientException('User confirmation failed')
    tokens = cognito_client.sign_in(user_id)
    creds = cognito_client.get_identity_pool_credentials(user_id, tokens['IdToken'])
    return {
        'tokens': tokens,
        'credentials': creds,
    }
