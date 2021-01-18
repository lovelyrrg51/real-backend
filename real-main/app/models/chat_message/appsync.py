import logging

import gql

logger = logging.getLogger()


class ChatMessageAppSync:
    def __init__(self, appsync_client):
        self.client = appsync_client

    def trigger_notification(self, notification_type, user_id, message):
        mutation = gql.gql(
            '''
            mutation TriggerChatMessageNotification ($input: ChatMessageNotificationInput!) {
                triggerChatMessageNotification (input: $input) {
                    userId
                    type
                    message {
                        messageId
                        chat {
                            chatId
                        }
                        authorUserId
                        author {
                            userId
                            username
                            photo {
                                url64p
                            }
                        }
                        text
                        textTaggedUsers {
                            tag
                            user {
                                userId
                            }
                        }
                        createdAt
                        lastEditedAt
                    }
                }
            }
        '''
        )
        input_obj = {
            'userId': user_id,
            'messageId': message.id,
            'chatId': message.chat_id,
            'authorUserId': message.user_id,
            'authorEncoded': message.get_author_encoded(user_id),
            'type': notification_type,
            'text': message.item['text'],
            'textTaggedUserIds': message.item.get('textTags', []),
            'createdAt': message.item['createdAt'],
            'lastEditedAt': message.item.get('lastEditedAt'),
        }
        self.client.send(mutation, {'input': input_obj})
