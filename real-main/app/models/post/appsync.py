import logging

import gql

logger = logging.getLogger()


class PostAppSync:
    def __init__(self, appsync_client):
        self.client = appsync_client

    def trigger_notification(self, notification_type, post):
        mutation = gql.gql(
            '''
            mutation TriggerPostNotification ($input: PostNotificationInput!) {
                triggerPostNotification (input: $input) {
                    userId
                    type
                    post {
                        postId
                        postStatus
                        isVerified
                    }
                }
            }
        '''
        )
        input_obj = {
            'userId': post.user_id,
            'type': notification_type,
            'postId': post.id,
            'postStatus': post.status,
            'isVerified': post.item.get('isVerified'),
        }
        self.client.send(mutation, {'input': input_obj})
