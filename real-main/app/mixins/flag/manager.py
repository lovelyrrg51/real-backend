import logging

from .dynamo import FlagDynamo

logger = logging.getLogger()


class FlagManagerMixin:
    # users that have flagging superpowers
    flag_admin_usernames = ('real', 'ian')

    def __init__(self, clients, managers=None):
        super().__init__(clients, managers=managers)
        if 'dynamo' in clients:
            self.flag_dynamo = FlagDynamo(self.item_type, clients['dynamo'])

    def on_flag_add(self, item_id, new_item):
        raise NotImplementedError('Subclasses must implement')

    def on_flag_delete(self, item_id, old_item):
        self.dynamo.decrement_flag_count(item_id)

    def on_item_delete_delete_flags(self, item_id, old_item):
        key_generator = self.flag_dynamo.generate_keys_by_item(item_id)
        self.dynamo.client.batch_delete_items(key_generator)

    def on_user_delete_delete_flags(self, user_id, old_item):
        # flagCounts on the item are decremented by post-delete dynamo handler
        key_generator = self.flag_dynamo.generate_keys_by_user(user_id)
        self.dynamo.client.batch_delete_items(key_generator)
