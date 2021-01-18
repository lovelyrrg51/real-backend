import logging
from itertools import chain

from app import models
from app.models.user.enums import UserPrivacyStatus, UserStatus
from app.utils import GqlNotificationType

from .dynamo.base import FollowerDynamo
from .dynamo.first_story import FirstStoryDynamo
from .enums import FollowStatus
from .exceptions import FollowerAlreadyExists, FollowerException
from .model import Follower

logger = logging.getLogger()


class FollowerManager:
    def __init__(self, clients, managers=None):
        managers = managers or {}
        managers['follower'] = self
        self.block_manager = managers.get('block') or models.BlockManager(clients, managers=managers)
        self.post_manager = managers.get('post') or models.PostManager(clients, managers=managers)

        self.clients = clients
        if 'appsync' in clients:
            self.appsync_client = clients['appsync']
        if 'dynamo' in clients:
            self.dynamo = FollowerDynamo(clients['dynamo'])
            self.first_story_dynamo = FirstStoryDynamo(clients['dynamo'])

    def get_follow(self, follower_user_id, followed_user_id, strongly_consistent=False):
        item = self.dynamo.get_following(
            follower_user_id, followed_user_id, strongly_consistent=strongly_consistent
        )
        return self.init_follow(item) if item else None

    def init_follow(self, follow_item):
        return Follower(follow_item, self.dynamo, self.first_story_dynamo)

    def get_follow_status(self, follower_user_id, followed_user_id):
        if follower_user_id == followed_user_id:
            return FollowStatus.SELF
        follow = self.get_follow(follower_user_id, followed_user_id)
        if not follow:
            return FollowStatus.NOT_FOLLOWING
        return follow.status

    def generate_follower_user_ids(self, followed_user_id, follow_status=None):
        "Return a generator that produces user ids of users that follow the given user"
        gen = self.dynamo.generate_follower_items(followed_user_id, follow_status=follow_status)
        gen = map(lambda item: item['followerUserId'], gen)
        return gen

    def generate_followed_user_ids(self, follower_user_id, follow_status=None):
        "Return a generator that produces user ids of users given user follows"
        gen = self.dynamo.generate_followed_items(follower_user_id, follow_status=follow_status)
        gen = map(lambda item: item['followedUserId'], gen)
        return gen

    def request_to_follow(self, follower_user, followed_user):
        "Returns the status of the follow request"
        if followed_user.status != UserStatus.ACTIVE:
            raise FollowerException(f'Cannot follow user with status `{followed_user.status}`')

        if self.get_follow(follower_user.id, followed_user.id):
            raise FollowerAlreadyExists(follower_user.id, followed_user.id)

        # can't follow a user that has blocked us
        if self.block_manager.is_blocked(followed_user.id, follower_user.id):
            raise FollowerException(f'User has been blocked by user `{followed_user.id}`')

        # can't follow a user we have blocked
        if self.block_manager.is_blocked(follower_user.id, followed_user.id):
            raise FollowerException(f'User has blocked user `{followed_user.id}`')

        follow_status = (
            FollowStatus.REQUESTED
            if followed_user.item['privacyStatus'] == UserPrivacyStatus.PRIVATE
            else FollowStatus.FOLLOWING
        )
        follow_item = self.dynamo.add_following(follower_user.id, followed_user.id, follow_status)
        return self.init_follow(follow_item)

    def accept_all_requested_follow_requests(self, followed_user_id):
        for item in self.dynamo.generate_follower_items(followed_user_id, FollowStatus.REQUESTED):
            # can't batch this: dynamo doesn't support batch updates
            self.init_follow(item).accept()

    def delete_all_denied_follow_requests(self, followed_user_id):
        for item in self.dynamo.generate_follower_items(followed_user_id, FollowStatus.DENIED):
            # TODO: do as batch write
            self.dynamo.delete_following(item)

    def refresh_first_story(self, story_prev=None, story_now=None):
        "Refresh the firstStory items, if needed, after the a story has changed."
        assert story_prev or story_now
        if story_prev:
            assert 'expiresAt' in story_prev
        if story_now:
            assert 'expiresAt' in story_now
        if story_prev and story_now:
            assert story_prev['postId'] == story_now['postId']
        post_id = story_prev['postId'] if story_prev else story_now['postId']
        user_id = story_prev['postedByUserId'] if story_prev else story_now['postedByUserId']

        # dynamo query ordering not guaranteed,
        # so to make sure things are consistent we exclude the post we just operated on from this query
        db_story = self.post_manager.dynamo.get_next_completed_post_to_expire(user_id, exclude_post_id=post_id)

        # figgure out what the followed first story was prev, and is now, the operation we're refreshing for
        ffs_prev = next(
            iter(sorted(filter(lambda s: s is not None, [db_story, story_prev]), key=lambda s: s['expiresAt'])),
            None,
        )
        ffs_now = next(
            iter(sorted(filter(lambda s: s is not None, [db_story, story_now]), key=lambda s: s['expiresAt'])),
            None,
        )

        follower_uids_generator = self.generate_follower_user_ids(user_id, follow_status=FollowStatus.FOLLOWING)
        if ffs_prev and not ffs_now:
            # a story was deleted, and there are no more stories to take its place as ffs
            self.first_story_dynamo.delete_all(follower_uids_generator, user_id)

        if not ffs_prev and ffs_now:
            # there was no ffs, but a story was added and can now be ffs
            self.first_story_dynamo.set_all(follower_uids_generator, ffs_now)

        if ffs_prev and ffs_now:
            if ffs_prev != ffs_now:
                # the ffs has changed: either different post, or same post but that post changed
                self.first_story_dynamo.set_all(follower_uids_generator, ffs_now)

        if not ffs_prev and not ffs_now:
            raise AssertionError('Should be unreachable condition')

    def on_first_story_post_id_change_fire_gql_notifications(self, user_id, new_item=None, old_item=None):
        followed_user_id, follower_user_id = self.first_story_dynamo.parse_key(new_item or old_item)
        kwargs = {'followedUserId': followed_user_id}
        if new_item:
            kwargs['postId'] = new_item['postId']
        self.appsync_client.fire_notification(
            follower_user_id,
            GqlNotificationType.USER_FOLLOWED_USERS_WITH_STORIES_CHANGED,
            **kwargs,
        )

    def on_user_follow_status_change_sync_first_story(self, user_id, new_item=None, old_item=None):
        new_status = (new_item or {}).get('followStatus', FollowStatus.NOT_FOLLOWING)
        followed_user_id = user_id
        follower_user_id = (new_item or old_item)['sortKey'].split('/')[1]

        if new_status == FollowStatus.FOLLOWING:
            post = self.post_manager.dynamo.get_next_completed_post_to_expire(followed_user_id)
            if post:
                self.first_story_dynamo.set_all([follower_user_id], post)
        else:
            self.first_story_dynamo.delete_all([follower_user_id], followed_user_id)

    def on_user_delete_delete_follower_items(self, user_id, old_item):
        key_generator = chain(
            self.dynamo.generate_follower_items(user_id, keys_only=True),
            self.dynamo.generate_followed_items(user_id, keys_only=True),
        )
        self.dynamo.client.batch_delete(key_generator)
