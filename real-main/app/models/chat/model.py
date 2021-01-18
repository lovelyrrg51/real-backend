import logging

import pendulum

from app.mixins.flag.model import FlagModelMixin
from app.mixins.view.model import ViewModelMixin
from app.models.user.enums import UserStatus

from .enums import ChatType
from .exceptions import ChatException

logger = logging.getLogger()


class Chat(ViewModelMixin, FlagModelMixin):

    item_type = 'chat'

    def __init__(
        self,
        item,
        dynamo=None,
        member_dynamo=None,
        block_manager=None,
        chat_manager=None,
        chat_message_manager=None,
        user_manager=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if dynamo:
            self.dynamo = dynamo
        if member_dynamo:
            self.member_dynamo = member_dynamo
        if block_manager:
            self.block_manager = block_manager
        if chat_manager:
            self.chat_manager = chat_manager
        if chat_message_manager:
            self.chat_message_manager = chat_message_manager
        if user_manager:
            self.user_manager = user_manager

        self.item = item
        # immutables
        self.id = item['chatId']
        self.user_id = None  # this model has no 'owner' (required by ViewModelMixin)
        self.type = self.item['chatType']
        self.created_by_user_id = item['createdByUserId']

    def refresh_item(self, strongly_consistent=False):
        self.item = self.dynamo.get(self.id, strongly_consistent=strongly_consistent)
        return self

    def is_member(self, user_id):
        return bool(self.member_dynamo.get(self.id, user_id))

    def edit(self, edited_by_user, name=None):
        if self.type != ChatType.GROUP:
            raise ChatException(f'Cannot edit non-GROUP chat `{self.id}`')

        if name is None:
            return
        self.item = self.dynamo.update_name(self.id, name)
        self.chat_message_manager.add_system_message_group_name_edited(self.id, edited_by_user, name)
        self.item['messagesCount'] = self.item.get('messagesCount', 0) + 1
        return self

    def add(self, added_by_user, user_ids, now=None):
        now = now or pendulum.now('utc')
        if self.type != ChatType.GROUP:
            raise ChatException(f'Cannot add users to non-GROUP chat `{self.id}`')

        users = []
        for user_id in set(user_ids):

            # make sure the user exists, is ACTIVE
            user = self.user_manager.get_user(user_id)
            if not user:
                logger.warning(f'Cannot add non-existent user `{user_id}` to group chat `{self.id}`')
                continue
            if user.status != UserStatus.ACTIVE:
                logger.warning(
                    f'Refusing to add user `{user.id}` with status `{user.status}` to group chat `{self.id}`'
                )
                continue

            if added_by_user.id is not None:
                if user_id == added_by_user.id:
                    continue  # must already be in the chat

                if self.block_manager.is_blocked(added_by_user.id, user_id):
                    continue  # can't add a user you're blocking

                if self.block_manager.is_blocked(user_id, added_by_user.id):
                    continue  # can't add a user who is blocking you

                if not self.chat_manager.validate_dating_match_chat(user_id, added_by_user.id):
                    continue

            transacts = [
                self.member_dynamo.transact_add(self.id, user_id, now=now),
                self.dynamo.transact_increment_user_count(self.id),
            ]
            transact_exceptions = [
                ChatException(f'Unable to add chat membership of user `{user_id} in chat `{self.id}`'),
                Exception(f'Unable to increment Chat.userCount for chat `{self.id}`'),
            ]
            try:
                self.dynamo.client.transact_write_items(transacts, transact_exceptions)
            except ChatException:
                # user is already in the chat, nothing to do
                pass
            else:
                self.item['userCount'] = self.item.get('userCount', 0) + 1
                users.append(user)

        if users:
            self.chat_message_manager.add_system_message_added_to_group(self.id, added_by_user, users, now=now)
            self.item['messagesCount'] = self.item.get('messagesCount', 0) + 1

    def leave(self, user):
        if self.type != ChatType.GROUP:
            raise ChatException(f'Cannot leave non-GROUP chat `{self.id}`')

        # leave the chat
        transacts = [
            self.member_dynamo.transact_delete(self.id, user.id),
            self.dynamo.transact_decrement_user_count(self.id),
        ]
        transact_exceptions = [
            ChatException(f'Unable to delete chat membership of user `{user.id}` in chat `{self.id}`'),
            Exception(f'Unable to decrement Chat.userCount for chat `{self.id}`'),
        ]
        self.dynamo.client.transact_write_items(transacts, transact_exceptions)
        self.item['userCount'] -= 1

        # were we the last user in the chat? If so, clean up
        if self.item['userCount'] <= 0:
            self.delete()
        else:
            self.chat_message_manager.add_system_message_left_group(self.id, user)
            self.item['messagesCount'] = self.item.get('messagesCount', 0) + 1

        return self

    def flag(self, user):
        if not self.is_member(user.id):
            raise ChatException(f'User is not part of chat `{self.id}`')

        # write to the db
        self.flag_dynamo.add(self.id, user.id)
        self.item['flagCount'] = self.item.get('flagCount', 0) + 1

        # we don't call super() because that depends on the model having a 'user_id' property
        return self

    def is_crowdsourced_forced_removal_criteria_met(self):
        # force-delete the chat if at least 10% of the members of the chat have flagged it
        flag_count = self.item.get('flagCount', 0)
        user_count = self.item.get('userCount', 0)
        return flag_count > user_count / 10

    def delete(self):
        self.dynamo.delete(self.id)
