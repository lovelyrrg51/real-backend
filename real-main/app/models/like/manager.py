import logging

from app import models
from app.models.follower.enums import FollowStatus
from app.models.post.enums import PostStatus
from app.models.user.enums import UserPrivacyStatus

from .dynamo import LikeDynamo
from .enums import LikeStatus
from .exceptions import LikeException
from .model import Like

logger = logging.getLogger()


class LikeManager:
    def __init__(self, clients, managers=None):
        managers = managers or {}
        managers['like'] = self
        self.block_manager = managers.get('block') or models.BlockManager(clients, managers=managers)
        self.follower_manager = managers.get('follower') or models.FollowerManager(clients, managers=managers)
        self.post_manager = managers.get('post') or models.PostManager(clients, managers=managers)
        self.user_manager = managers.get('user') or models.UserManager(clients, managers=managers)

        self.clients = clients
        if 'dynamo' in clients:
            self.dynamo = LikeDynamo(clients['dynamo'])

    def get_like(self, user_id, post_id):
        like_item = self.dynamo.get_like(user_id, post_id)
        return self.init_like(like_item) if like_item else None

    def init_like(self, like_item):
        return Like(like_item, self.dynamo, post_manager=self.post_manager)

    def like_post(self, user, post, like_status, now=None):
        # can't like a post of a user that has blocked us
        if self.block_manager.is_blocked(post.user_id, user.id):
            raise LikeException(f'User has been blocked by owner of post `{post.id}`')

        # can't like a post of a user we have blocked
        if self.block_manager.is_blocked(user.id, post.user_id):
            raise LikeException(f'User has blocked owner of post `{post.id}`')

        # if the post is from a private user (other than ourselves) then we must be a follower to like the post
        posted_by_user = self.user_manager.get_user(post.user_id)
        if user.id != posted_by_user.id:
            if posted_by_user.item['privacyStatus'] != UserPrivacyStatus.PUBLIC:
                follow = self.follower_manager.get_follow(user.id, posted_by_user.id)
                if not follow or follow.status != FollowStatus.FOLLOWING:
                    raise LikeException(f'User does not have access to post `{post.id}`')

        if post.status != PostStatus.COMPLETED:
            raise LikeException(f'Cannot like posts with status `{post.status}`')

        if post.item.get('likesDisabled'):
            raise LikeException(f'Likes are disabled for this post `{post.id}`')

        if posted_by_user.item.get('likesDisabled'):
            raise LikeException(f'Owner of this post (user `{posted_by_user.id}` has disabled likes')

        if user.item.get('likesDisabled'):
            raise LikeException(f'Caller `{user.id}` has disabled likes')

        self.dynamo.add_like(user.id, post.item, like_status)
        # increment the correct like counter on the in-memory copy of the post
        attr = 'onymousLikeCount' if like_status == LikeStatus.ONYMOUSLY_LIKED else 'anonymousLikeCount'
        post.item[attr] = post.item.get(attr, 0) + 1

    def dislike_all_of_post(self, post_id):
        "Dislike all likes of a post"
        for like_item in self.dynamo.generate_of_post(post_id):
            self.init_like(like_item).dislike()

    def dislike_all_by_user_from_user(self, liked_by_user_id, posted_by_user_id):
        "Dislike all likes by one user on posts from another user"
        for like_pk in self.dynamo.generate_pks_by_liked_by_for_posted_by(liked_by_user_id, posted_by_user_id):
            liked_by_user_id, post_id = self.dynamo.parse_pk(like_pk)
            self.get_like(liked_by_user_id, post_id).dislike()

    def on_user_delete_dislike_all_by_user(self, user_id, old_item):
        "Dislike all likes by a user"
        for like_item in self.dynamo.generate_by_liked_by(user_id):
            self.init_like(like_item).dislike()

    def on_user_follow_status_change_sync_likes(self, user_id, new_item=None, old_item=None):
        "For consistency, delete likes of posts of private users by non-followers"
        new_status = (new_item or {}).get('followStatus', FollowStatus.NOT_FOLLOWING)
        followed_user_id = user_id
        follower_user_id = (new_item or old_item)['sortKey'].split('/')[1]

        if new_status == FollowStatus.DENIED:
            # we assume the followed user to be private because must be in order to get to DENIED
            self.dislike_all_by_user_from_user(follower_user_id, followed_user_id)

        if new_status == FollowStatus.NOT_FOLLOWING:
            # check to see if the followed user is private
            followed_user = self.user_manager.get_user(followed_user_id)
            if followed_user and followed_user.item.get('privacyStatus') == UserPrivacyStatus.PRIVATE:
                self.dislike_all_by_user_from_user(follower_user_id, followed_user_id)
