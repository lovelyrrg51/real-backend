import logging

import pendulum

logger = logging.getLogger()


class Card:
    def __init__(
        self,
        item,
        appsync=None,
        dynamo=None,
        pinpoint_client=None,
        post_manager=None,
        user_manager=None,
    ):
        self.appsync = appsync
        self.dynamo = dynamo
        self.pinpoint_client = pinpoint_client
        self.post_manager = post_manager
        self.user_manager = user_manager

        self.item = item
        # immutables
        self.id = item['partitionKey'][len('card/') :]
        self.post_id = item.get('postId')
        self.user_id = item['gsiA1PartitionKey'][len('user/') :]
        self.created_at = pendulum.parse(item['gsiA1SortKey'][len('card/') :])
        self.action = item['action']

    @property
    def user(self):
        if not hasattr(self, '_user'):
            self._user = self.user_manager.get_user(self.user_id)
        return self._user

    @property
    def post(self):
        if not hasattr(self, '_post'):
            self._post = self.post_manager.get_post(self.post_id) if self.post_id else None
        return self._post

    @property
    def title(self):
        return self.item['title']

    @property
    def sub_title(self):
        return self.item.get('subTitle')

    @property
    def notify_user_at(self):
        return pendulum.parse(self.item['gsiK1SortKey'].split('/')[0]) if 'gsiK1SortKey' in self.item else None

    def refresh_item(self, strongly_consistent=False):
        self.item = self.dynamo.get_card(self.id, strongly_consistent=strongly_consistent)
        return self

    def serialize(self, caller_user_id):
        resp = self.item.copy()
        resp['cardId'] = self.id
        return resp

    def trigger_notification(self, notification_type):
        self.appsync.trigger_notification(
            notification_type,
            self.user_id,
            self.id,
            self.title,
            self.action,
            sub_title=self.sub_title,
        )

    def notify_user(self):
        "Returns bool indicating if notification was successfully sent to user"
        # just APNS for now
        return self.pinpoint_client.send_user_apns(self.user_id, self.action, self.title, body=self.sub_title)

    def clear_notify_user_at(self):
        self.item = self.dynamo.clear_notify_user_at(self.id)
        return self

    def delete(self):
        self.dynamo.delete_card(self.id)
        return self
