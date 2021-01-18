import logging
import os
import re

from app.logging import LogLevelContext, handler_logging

logger = logging.getLogger()

COGNITO_TESTING_CLIENT_ID = os.environ.get('COGNITO_USER_POOL_TESTING_CLIENT_ID')

username_re = re.compile(r'us-east-1:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z')


class CognitoClientException(Exception):
    pass


@handler_logging(event_to_extras=lambda event: {'event': event})
def pre_sign_up(event, context):
    with LogLevelContext(logger, logging.INFO):
        logger.info('Handling Cognito PreSignUp event')

    validate_username_format(event)
    validate_user_attribute_lowercase(event, 'email')

    # AWS doesn't let you set preferred_username in this call because the user isn't confirmed yet
    # validate_user_attribute_lowercase(event, 'preferred_username')

    client_id = event['callerContext']['clientId']
    if client_id == COGNITO_TESTING_CLIENT_ID:
        # make sure users created by the testing client are marked as such
        # so they can be identified and deleted later on, if testing cleanup doesn't catch them
        family_name = get_user_attribute(event, 'family_name')
        if family_name != 'TESTER':
            raise CognitoClientException(f'Invalid family_name: `{family_name}`')

        # testing client is allowed to optionally auto-confirm & verify users
        # so they can login without receiving an email/text
        if (event['request'].get('clientMetadata') or {}).get('autoConfirmUser'):
            event['response']['autoConfirmUser'] = True
            if get_user_attribute(event, 'email'):
                event['response']['autoVerifyEmail'] = True
            if get_user_attribute(event, 'phone_number'):
                event['response']['autoVerifyPhone'] = True

    return event


@handler_logging(event_to_extras=lambda event: {'event': event})
def pre_auth(event, context):
    with LogLevelContext(logger, logging.INFO):
        logger.info('Handling Cognito PreAuth event')

    # if the user doesn't exist in the user pool or is unconfirmed
    # cognito appears to create a random uuid as their 'userName'
    validate_user_attribute_lowercase(event, 'email')
    validate_user_attribute_lowercase(event, 'preferred_username')
    return event


@handler_logging(event_to_extras=lambda event: {'event': event})
def custom_message(event, context):
    with LogLevelContext(logger, logging.INFO):
        logger.info('Handling Cognito CustomMessage event')

    if event['triggerSource'] in ('CustomMessage_SignUp', 'CustomMessage_ResendCode'):
        user_id = event['userName']
        code = event['request']['codeParameter']
        deepurl = f'https://real.app/confirm/email/{user_id}/{code}'
        event['response']['smsMessage'] = f'Welcome to REAL. Your confirmation code is {code}'
        event['response']['emailSubject'] = 'Welcome to REAL'
        event['response']['emailMessage'] = (
            f'Welcome to REAL. Tap <a href="{deepurl}">here</a> to confirm your account. '
            f'Should you need it, your confirmation code is {code}.'
        )

    if event['triggerSource'] == 'CustomMessage_ForgotPassword':
        user_id = event['userName']
        code = event['request']['codeParameter']
        deepurl = f'https://real.app/confirm/forgot/{user_id}/{code}'
        event['response']['smsMessage'] = f'Your REAL password reset code is {code}'
        event['response']['emailSubject'] = 'Your REAL password reset link'
        event['response']['emailMessage'] = (
            f'Tap <a href="{deepurl}">here</a> to choose a new REAL password. '
            f'Should you need it, your password reset code is {code}.'
        )

    if event['triggerSource'] in ('CustomMessage_UpdateUserAttribute', 'CustomMessage_VerifyUserAttribute'):
        user_id = event['userName']
        code = event['request']['codeParameter']
        deepurl = f'https://real.app/confirm/email/{user_id}/{code}'
        event['response']['smsMessage'] = f'Your REAL confirmation code is {code}'
        event['response']['emailSubject'] = 'Your REAL confirmation link'
        event['response']['emailMessage'] = (
            f'Tap <a href="{deepurl}">here</a> to confirm your email address with REAL. '
            f'Should you need it, your confirmation code is {code}.'
        )

    return event


@handler_logging(event_to_extras=lambda event: {'event': event})
def define_auth_challenge(event, context):
    with LogLevelContext(logger, logging.INFO):
        logger.info('Handling Cognito DefineAuthChallenge event')
    # Log the user in, no need to challenge them. Note that
    # custom auth is restricted to only the backend user pool client
    event['response']['issueTokens'] = True
    return event


def get_user_attribute(event, attr_name):
    return (event['request'].get('userAttributes') or {}).get(attr_name)


def validate_username_format(event):
    cognito_username = event.get('userName', '')
    if not username_re.match(cognito_username):
        raise CognitoClientException(f'Invalid username format: `{cognito_username}`')


def validate_user_attribute_lowercase(event, attr_name):
    "If value is present, ensure it is lowercase. Passes if attribute is missing"
    attr = get_user_attribute(event, attr_name)
    if attr and any([c.isupper() for c in attr]):
        raise CognitoClientException(f"User's {attr_name} '{attr}' has upper case characters")
