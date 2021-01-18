import logging
import uuid

import pendulum

from app import models
from app.clients import BadWordsClient
from app.mixins.base import ManagerBase
from app.mixins.flag.manager import FlagManagerMixin
from app.models.chat.enums import ChatType
from app.models.follower.enums import FollowStatus

from .appsync import ChatMessageAppSync
from .dynamo import ChatMessageDynamo
from .enums import ChatMessageNotificationType
from .model import ChatMessage

logger = logging.getLogger()


class ChatMessageManager(FlagManagerMixin, ManagerBase):

    item_type = 'chatMessage'

    def __init__(self, clients, managers=None):
        super().__init__(clients, managers=managers)
        managers = managers or {}
        managers['chat_message'] = self
        self.block_manager = managers.get('block') or models.BlockManager(clients, managers=managers)
        self.chat_manager = managers.get('chat') or models.ChatManager(clients, managers=managers)
        self.user_manager = managers.get('user') or models.UserManager(clients, managers=managers)
        self.follower_manager = managers.get('follower') or models.FollowerManager(clients, managers=managers)

        self.clients = clients
        if 'appsync' in clients:
            self.appsync = ChatMessageAppSync(clients['appsync'])
        if 'dynamo' in clients:
            self.dynamo = ChatMessageDynamo(clients['dynamo'])

    def get_model(self, item_id, strongly_consistent=False):
        return self.get_chat_message(item_id, strongly_consistent=strongly_consistent)

    def get_chat_message(self, message_id, strongly_consistent=False):
        item = self.dynamo.get_chat_message(message_id, strongly_consistent=strongly_consistent)
        return self.init_chat_message(item) if item else None

    def init_chat_message(self, item):
        kwargs = {
            'chat_message_appsync': self.appsync,
            'chat_message_dynamo': self.dynamo,
            'flag_dynamo': getattr(self, 'flag_dynamo', None),
            'block_manager': self.block_manager,
            'chat_manager': self.chat_manager,
            'user_manager': self.user_manager,
            'follower_manager': self.follower_manager,
        }
        return ChatMessage(item, **kwargs)

    def add_chat_message(self, message_id, text, chat_id, user_id, now=None):
        now = now or pendulum.now('utc')
        text_tags = self.user_manager.get_text_tags(text)
        item = self.dynamo.add_chat_message(message_id, chat_id, user_id, text, text_tags, now)
        return self.init_chat_message(item)

    def add_system_message_group_created(self, chat_id, created_by_user, name=None, now=None):
        text = f'@{created_by_user.username} created the group'
        if name:
            text += f' "{name}"'
        return self.add_system_message(chat_id, text, user_ids=[created_by_user.id], now=now)

    def add_system_message_added_to_group(self, chat_id, added_by_user, users, now=None):
        assert users, 'No system message should be sent if no users added to group'
        text = f'@{added_by_user.username} added '
        user_1 = users.pop()
        if users:
            text += ', '.join(f'@{u.username}' for u in users)
            text += ' and '
        text += f'@{user_1.username} to the group'
        return self.add_system_message(chat_id, text, user_ids=[u.id for u in users], now=now)

    def add_system_message_left_group(self, chat_id, user):
        text = f'@{user.username} left the group'
        return self.add_system_message(chat_id, text)

    def add_system_message_group_name_edited(self, chat_id, changed_by_user, new_name):
        text = f'@{changed_by_user.username} '
        if new_name:
            text += f'changed the name of the group to "{new_name}"'
        else:
            text += 'deleted the name of the group'
        return self.add_system_message(chat_id, text)

    def add_system_message(self, chat_id, text, user_ids=None, now=None):
        user_id = None
        message_id = str(uuid.uuid4())
        message = self.add_chat_message(message_id, text, chat_id, user_id, now=now)
        message.trigger_notifications(ChatMessageNotificationType.ADDED, user_ids=user_ids)
        return message

    def clear_chat_message_bad_words(self):
        # scan for all chat messages and detect bad words
        for message in self.dynamo.generate_all_chat_messages_by_scan():
            self.on_chat_message_changed_detect_bad_words(message['messageId'], message)

    def on_flag_add(self, message_id, new_item):
        chat_message_item = self.dynamo.increment_flag_count(message_id)
        chat_message = self.init_chat_message(chat_message_item)

        # force delete the chat_message?
        if chat_message.is_crowdsourced_forced_removal_criteria_met():
            logger.warning(f'Force deleting chat message `{message_id}` from flagging')
            chat_message.delete(forced=True)

    def on_chat_message_changed_detect_bad_words(self, message_id, new_item, old_item=None):
        text = new_item['text']
        chat_message = self.init_chat_message(new_item)
        chat = chat_message.chat

        if chat_message.user_id is None:  # system message
            return

        user_ids = chat.member_dynamo.generate_user_ids_by_chat(chat.id)
        if not self.should_process_bad_words_detection(chat.type, chat_message.user_id, user_ids):
            return

        # if detects bad words, force delete the chat message
        bad_words_client = BadWordsClient()
        if bad_words_client.validate_bad_words_detection(text):
            logger.warning(f'Force deleting chat message `{message_id}` from detecting bad words')
            chat_message.delete(forced=True)

    def on_chat_delete_delete_messages(self, chat_id, old_item):
        generator = self.dynamo.generate_chat_messages_by_chat(chat_id, pks_only=True)
        self.dynamo.client.batch_delete_items(generator)

    def should_process_bad_words_detection(self, chat_type, chat_message_creator_id, user_ids):
        # if direct chat and they are 2 way follow, skip
        # if group chat and all users in the chat follow the user creating the message with the bad word, skip
        for user_id in user_ids:
            if user_id == chat_message_creator_id:
                continue

            follow = self.follower_manager.get_follow(user_id, chat_message_creator_id)
            follow_back = self.follower_manager.get_follow(chat_message_creator_id, user_id)

            if chat_type == ChatType.DIRECT:
                if (
                    follow
                    and follow_back
                    and follow.status == FollowStatus.FOLLOWING
                    and follow_back.status == FollowStatus.FOLLOWING
                ):
                    return False
            else:
                if not follow or follow.status != FollowStatus.FOLLOWING:
                    return True

        return True if chat_type == ChatType.DIRECT else False
