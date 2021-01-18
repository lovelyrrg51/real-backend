import logging

from .dynamo import ViewDynamo

logger = logging.getLogger()


class ViewManagerMixin:
    def __init__(self, clients, managers=None):
        super().__init__(clients, managers=managers)
        if 'dynamo' in clients:
            self.view_dynamo = ViewDynamo(self.item_type, clients['dynamo'])

    def record_views(self, item_ids, user_id, viewed_at=None):
        raise NotImplementedError  # subclasses must implement

    def on_item_delete_delete_views(self, item_id, old_item):
        key_gen = self.view_dynamo.generate_keys_by_item(item_id)
        self.view_dynamo.client.batch_delete_items(key_gen)

    def on_user_delete_delete_views(self, user_id, old_item):
        key_gen = self.view_dynamo.generate_keys_by_user(user_id)
        self.view_dynamo.client.batch_delete_items(key_gen)
