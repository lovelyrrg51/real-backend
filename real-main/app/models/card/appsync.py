import logging

import gql

logger = logging.getLogger()


class CardAppSync:
    def __init__(self, appsync_client):
        self.client = appsync_client

    def trigger_notification(self, notification_type, user_id, card_id, title, action, sub_title=None):
        mutation = gql.gql(
            '''
            mutation TriggerCardNotification ($input: CardNotificationInput!) {
                triggerCardNotification (input: $input) {
                    userId
                    type
                    card {
                        cardId
                        title
                        subTitle
                        action
                    }
                }
            }
        '''
        )
        input_obj = {
            'userId': user_id,
            'type': notification_type,
            'cardId': card_id,
            'title': title,
            'subTitle': sub_title,
            'action': action,
        }
        self.client.send(mutation, {'input': input_obj})
