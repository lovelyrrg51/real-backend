import logging

import pendulum

from app import clients, models
from app.logging import LogLevelContext, handler_logging

from . import xray

logger = logging.getLogger()
xray.patch_all()

clients = {
    'appsync': clients.AppSyncClient(),
    'dynamo': clients.DynamoClient(),
    'cognito': clients.CognitoClient(),
    'pinpoint': clients.PinpointClient(),
}

managers = {}
chat_manager = managers.get('chat') or models.ChatManager(clients, managers=managers)
chat_message_manager = managers.get('chat_message') or models.ChatMessageManager(clients, managers=managers)
user_manager = managers.get('user') or models.UserManager(clients, managers=managers)


@handler_logging(event_to_extras=lambda event: {'event': event})
def create_dating_chat(event, context):
    with LogLevelContext(logger, logging.INFO):
        logger.info('create_dating_chat() called')

    user_id = event['userId']
    chat_id = event['chatId']
    match_user_id = event['matchUserId']
    message_text = event['messageText']

    # Create direct chat with system message
    now = pendulum.now('utc')
    chat = chat_manager.add_direct_chat(chat_id, user_id, match_user_id, now=now)
    chat_message_manager.add_system_message(chat_id, message_text, user_ids=[user_id, match_user_id], now=now)

    chat.refresh_item(strongly_consistent=True)
    return chat.item
