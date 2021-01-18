import logging

from app import models
from app.models.user.enums import UserStatus

from .dynamo import BlockDynamo
from .enums import BlockStatus
from .exceptions import BlockException, NotBlocked

logger = logging.getLogger()


class BlockManager:
    def __init__(self, clients, managers=None):
        managers = managers or {}
        managers['block'] = self
        self.chat_manager = managers.get('chat') or models.ChatManager(clients, managers=managers)
        self.follower_manager = managers.get('follower') or models.FollowerManager(clients, managers=managers)
        self.like_manager = managers.get('like') or models.LikeManager(clients, managers=managers)
        self.user_manager = managers.get('user') or models.UserManager(clients, managers=managers)

        self.clients = clients
        if 'dynamo' in clients:
            self.dynamo = BlockDynamo(clients['dynamo'])

    def is_blocked(self, blocker_user_id, blocked_user_id):
        block_item = self.dynamo.get_block(blocker_user_id, blocked_user_id)
        return bool(block_item)

    def get_block_status(self, blocker_user_id, blocked_user_id):
        if blocker_user_id == blocked_user_id:
            return BlockStatus.SELF
        block_item = self.dynamo.get_block(blocker_user_id, blocked_user_id)
        return BlockStatus.BLOCKING if block_item else BlockStatus.NOT_BLOCKING

    def block(self, blocker_user, blocked_user):
        block_item = self.dynamo.add_block(blocker_user.id, blocked_user.id)

        if blocked_user.status != UserStatus.ACTIVE:
            raise BlockException(f'Cannot block user with status `{blocked_user.status}`')

        # force-unfollow them if we're following them
        follow = self.follower_manager.get_follow(blocker_user.id, blocked_user.id)
        if follow:
            follow.unfollow(force=True)

        # force-unfollow us if they're following us
        follow = self.follower_manager.get_follow(blocked_user.id, blocker_user.id)
        if follow:
            follow.unfollow(force=True)

        # force-dislike any likes of posts between the two of us
        self.like_manager.dislike_all_by_user_from_user(blocker_user.id, blocked_user.id)
        self.like_manager.dislike_all_by_user_from_user(blocked_user.id, blocker_user.id)

        # if a direct chat between the user exists, delete it
        chat = self.chat_manager.get_direct_chat(blocked_user.id, blocker_user.id)
        if chat:
            chat.delete()

        return block_item

    def unblock(self, blocker_user, blocked_user):
        deleted_item = self.dynamo.delete_block(blocker_user.id, blocked_user.id)
        if not deleted_item:
            raise NotBlocked(blocker_user.id, blocked_user.id)
        return deleted_item

    def on_user_delete_unblock_all_blocks(self, user_id, old_item):
        "Unblock everyone who the user has blocked, or has blocked the user"
        self.dynamo.delete_all_blocks_by_user(user_id)
        self.dynamo.delete_all_blocks_of_user(user_id)

    def on_user_blocked_sync_user_status(self, user_id, new_item):
        blocker_user_id = new_item['sortKey'].split('/')[1]
        if blocker_user_id == self.user_manager.real_user_id:
            user = self.user_manager.get_user(user_id)
            if user:
                user.disable(forced_by='blocked by REAL user')
