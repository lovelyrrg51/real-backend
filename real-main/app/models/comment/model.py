import logging

import pendulum

from app.mixins.flag.model import FlagModelMixin
from app.models.follower.enums import FollowStatus
from app.models.user.enums import UserPrivacyStatus

from .exceptions import CommentException

logger = logging.getLogger()


class Comment(FlagModelMixin):

    item_type = 'comment'

    def __init__(
        self,
        comment_item,
        dynamo=None,
        block_manager=None,
        follower_manager=None,
        post_manager=None,
        user_manager=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if dynamo:
            self.dynamo = dynamo
        if block_manager:
            self.block_manager = block_manager
        if follower_manager:
            self.follower_manager = follower_manager
        if post_manager:
            self.post_manager = post_manager
        if user_manager:
            self.user_manager = user_manager

        self.item = comment_item
        self.id = comment_item['commentId']
        self.user_id = comment_item['userId']
        self.post_id = comment_item['postId']
        self.created_at = pendulum.parse(comment_item['commentedAt'])

    @property
    def post(self):
        if not hasattr(self, '_post'):
            self._post = self.post_manager.get_post(self.post_id)
        return self._post

    @property
    def user(self):
        if not hasattr(self, '_user'):
            self._user = self.user_manager.get_user(self.user_id)
        return self._user

    @property
    def viewed_by_count(self):
        return self.post.item.get('viewedByCount', 0)

    def refresh_item(self, strongly_consistent=False):
        self.item = self.dynamo.get_comment(self.id, strongly_consistent=strongly_consistent)
        return self

    def serialize(self, caller_user_id):
        resp = self.item.copy()
        resp['commentedBy'] = self.user_manager.get_user(self.user_id).serialize(caller_user_id)
        return resp

    def delete(self, deleter_user_id=None, forced=False):
        # users may only delete their own comments or comments on their posts
        if deleter_user_id and deleter_user_id not in (self.post.user_id, self.user_id):
            raise CommentException(f'User is not authorized to delete comment `{self.id}`')
        self.dynamo.delete_comment(self.id)
        if forced:
            self.user_manager.dynamo.increment_comment_forced_deletion_count(self.user_id)
        return self

    def flag(self, user):
        # if comment is on a post is from a private user then we must be a follower of the post owner
        posted_by_user = self.user_manager.get_user(self.post.user_id)
        if user.id != posted_by_user.id and posted_by_user.item['privacyStatus'] != UserPrivacyStatus.PUBLIC:
            follow = self.follower_manager.get_follow(user.id, self.user_id)
            if not follow or follow.status != FollowStatus.FOLLOWING:
                raise CommentException(f'User does not have access to comment `{self.id}`')
        return super().flag(user)
