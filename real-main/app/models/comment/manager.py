import json
import logging

import pendulum

from app import models
from app.clients import BadWordsClient, RealDatingClient
from app.mixins.base import ManagerBase
from app.mixins.flag.manager import FlagManagerMixin
from app.models.follower.enums import FollowStatus
from app.models.post.enums import PostType
from app.models.user.enums import UserPrivacyStatus

from .dynamo import CommentDynamo
from .exceptions import CommentException
from .model import Comment

logger = logging.getLogger()


class CommentManager(FlagManagerMixin, ManagerBase):

    item_type = 'comment'

    def __init__(self, clients, managers=None):
        super().__init__(clients, managers=managers)
        managers = managers or {}
        managers['comment'] = self
        self.block_manager = managers.get('block') or models.BlockManager(clients, managers=managers)
        self.follower_manager = managers.get('follower') or models.FollowerManager(clients, managers=managers)
        self.post_manager = managers.get('post') or models.PostManager(clients, managers=managers)
        self.user_manager = managers.get('user') or models.UserManager(clients, managers=managers)

        self.real_dating_client = RealDatingClient()
        # self.bad_words_client = BadWordsClient()
        if 'dynamo' in clients:
            self.dynamo = CommentDynamo(clients['dynamo'])

    def get_model(self, item_id):
        return self.get_comment(item_id)

    def get_comment(self, comment_id):
        comment_item = self.dynamo.get_comment(comment_id)
        return self.init_comment(comment_item) if comment_item else None

    def init_comment(self, comment_item):
        kwargs = {
            'dynamo': getattr(self, 'dynamo', None),
            'flag_dynamo': getattr(self, 'flag_dynamo', None),
            'block_manager': self.block_manager,
            'follower_manager': self.follower_manager,
            'post_manager': self.post_manager,
            'user_manager': self.user_manager,
        }
        return Comment(comment_item, **kwargs)

    def add_comment(self, comment_id, post_id, user_id, text, now=None):
        now = now or pendulum.now('utc')

        post = self.post_manager.get_post(post_id)
        if not post:
            raise CommentException(f'Post `{post_id}` does not exist')

        if post.item.get('commentsDisabled', False):
            raise CommentException(f'Comments are disabled on post `{post_id}`')

        if user_id != post.user_id:

            # can't comment if there's a blocking relationship, either direction
            if self.block_manager.is_blocked(post.user_id, user_id):
                raise CommentException(f'Post owner `{post.user_id}` has blocked user `{user_id}`')
            if self.block_manager.is_blocked(user_id, post.user_id):
                raise CommentException(f'User `{user_id}` has blocked post owner `{post.user_id}`')

            # if post owner is private, must be a follower to comment
            poster = self.user_manager.get_user(post.user_id)
            if poster.item['privacyStatus'] == UserPrivacyStatus.PRIVATE:
                follow = self.follower_manager.get_follow(user_id, post.user_id)
                if not follow or follow.status != FollowStatus.FOLLOWING:
                    msg = f'Post owner `{post.user_id}` is private and user `{user_id}` is not a follower'
                    raise CommentException(msg)

            # can't add a comment if the match status is not CONFIRMED, only post type is IMAGE
            if post.type == PostType.IMAGE:
                if not self.validate_dating_match_comment(user_id, post.user_id):
                    raise CommentException('Cannot add comment unless it is a confirmed match on dating')

        text_tags = self.user_manager.get_text_tags(text)
        comment_item = self.dynamo.add_comment(comment_id, post_id, user_id, text, text_tags, commented_at=now)
        return self.init_comment(comment_item)

    def clear_comment_bad_words(self):
        # scan for all comment texts and detect bad words
        for comment in self.dynamo.generate_all_comments_by_scan():
            self.on_comment_added_detect_bad_words(comment['commentId'], comment)

    def on_user_delete_delete_all_by_user(self, user_id, old_item):
        for comment_item in self.dynamo.generate_by_user(user_id):
            self.init_comment(comment_item).delete()

    def delete_all_on_post(self, post_id):
        for comment_item in self.dynamo.generate_by_post(post_id):
            self.init_comment(comment_item).delete()

    def on_flag_add(self, comment_id, new_item):
        comment_item = self.dynamo.increment_flag_count(comment_id)
        comment = self.init_comment(comment_item)

        user_id = new_item['sortKey'].split('/')[1]
        flagger = self.user_manager.get_user(user_id)

        # force delete the comment?
        if (
            flagger.id == comment.post.user_id
            or flagger.username in self.flag_admin_usernames
            or comment.is_crowdsourced_forced_removal_criteria_met()
        ):
            logger.warning(f'Force deleting comment `{comment_id}` from flagging')
            comment.delete(forced=True)

    def on_comment_added_detect_bad_words(self, comment_id, new_item):
        text = new_item['text']
        comment = self.init_comment(new_item)

        # if they are 2 way follow, skip bad words detection
        post = self.post_manager.get_post(comment.post_id)
        if comment.user_id != post.user_id:
            follow = self.follower_manager.get_follow(comment.user_id, post.user_id)
            follow_back = self.follower_manager.get_follow(post.user_id, comment.user_id)

            if (
                follow
                and follow_back
                and follow.status == FollowStatus.FOLLOWING
                and follow_back.status == FollowStatus.FOLLOWING
            ):
                return

        # if detects bad words, force delete the comment
        bad_words_client = BadWordsClient()
        if bad_words_client.validate_bad_words_detection(text):
            logger.warning(f'Force deleting comment `{comment_id}` from detecting bad words')
            comment.delete(forced=True)

    def validate_dating_match_comment(self, user_id, match_user_id):
        response_1 = json.loads(
            self.real_dating_client.match_status(user_id, match_user_id)['Payload'].read().decode()
        )
        response_2 = json.loads(
            self.real_dating_client.match_status(user_id=match_user_id, match_user_id=user_id)['Payload']
            .read()
            .decode()
        )
        match_status_1 = response_1['status']
        match_status_2 = response_2['status']
        # we can reference blockChatExpiredAt to block adding comment
        blockChatExpiredAt = response_1['blockChatExpiredAt']

        if match_status_1 != 'CONFIRMED' or match_status_2 != 'CONFIRMED':
            if (
                blockChatExpiredAt is not None and pendulum.parse(blockChatExpiredAt) > pendulum.now()
            ):  # 30 days blocking comment
                return False
        return True
