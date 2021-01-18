import logging

from .exceptions import FlagException

logger = logging.getLogger()


class FlagModelMixin:
    def __init__(self, flag_dynamo=None, **kwargs):
        super().__init__(**kwargs)
        if flag_dynamo:
            self.flag_dynamo = flag_dynamo

    def flag(self, user):
        # can't flag a model of a user that has blocked us
        if self.block_manager.is_blocked(self.user_id, user.id):
            raise FlagException(f'User has been blocked by owner of {self.item_type} `{self.id}`')

        # can't flag a model of a user we have blocked
        if self.block_manager.is_blocked(user.id, self.user_id):
            raise FlagException(f'User has blocked owner of {self.item_type} `{self.id}`')

        # cant flag our own model
        if user.id == self.user_id:
            raise FlagException(f'User cant flag their own {self.item_type} `{self.id}`')

        # write to the db
        self.flag_dynamo.add(self.id, user.id)
        self.item['flagCount'] = self.item.get('flagCount', 0) + 1
        return self

    def unflag(self, user_id):
        self.flag_dynamo.delete(self.id, user_id)
        self.item['flagCount'] = self.item.get('flagCount', 0) - 1
        return self

    def is_crowdsourced_forced_removal_criteria_met(self):
        # the item should be force-archived if (directly from spec):
        #   - over 5 users have viewed the item and
        #   - at least 10% of them have flagged it
        flag_count = self.item.get('flagCount', 0)
        return self.viewed_by_count > 5 and flag_count > self.viewed_by_count / 10
