import logging

from .enums import FollowStatus
from .exceptions import FollowerAlreadyHasStatus

logger = logging.getLogger()


class Follower:
    def __init__(self, follow_item, follow_dynamo, first_story_dynamo):
        self.dynamo = follow_dynamo
        self.first_story_dynamo = first_story_dynamo
        self.followed_user_id = follow_item['followedUserId']
        self.follower_user_id = follow_item['followerUserId']
        self.item = follow_item

    @property
    def status(self):
        return self.item['followStatus'] if self.item else FollowStatus.NOT_FOLLOWING

    def refresh_item(self):
        self.item = self.dynamo.get_following(self.follower_user_id, self.followed_user_id)
        return self

    def unfollow(self, force=False):
        "Returns the status of the follow request"
        if not force and self.status == FollowStatus.DENIED:
            raise FollowerAlreadyHasStatus(self.follower_user_id, self.followed_user_id, FollowStatus.DENIED)
        self.dynamo.delete_following(self.item)
        self.item['followStatus'] = FollowStatus.NOT_FOLLOWING
        return self

    def accept(self):
        "Returns the status of the follow request"
        if self.status == FollowStatus.FOLLOWING:
            raise FollowerAlreadyHasStatus(self.follower_user_id, self.followed_user_id, FollowStatus.FOLLOWING)
        self.item = self.dynamo.update_following_status(self.item, FollowStatus.FOLLOWING)
        return self

    def deny(self):
        "Returns the status of the follow request"
        if self.status == FollowStatus.DENIED:
            raise FollowerAlreadyHasStatus(self.follower_user_id, self.followed_user_id, FollowStatus.DENIED)
        self.item = self.dynamo.update_following_status(self.item, FollowStatus.DENIED)
        return self
