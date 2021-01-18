import collections
import itertools
import logging

import pendulum

from app import models
from app.mixins.base import ManagerBase
from app.mixins.flag.manager import FlagManagerMixin
from app.mixins.trending.manager import TrendingManagerMixin
from app.mixins.view.enums import ViewType
from app.mixins.view.manager import ViewManagerMixin
from app.models.like.enums import LikeStatus
from app.utils import GqlNotificationType

from .appsync import PostAppSync
from .dynamo import PostDynamo, PostImageDynamo, PostOriginalMetadataDynamo
from .enums import PostStatus, PostType
from .exceptions import PostException
from .model import Post

logger = logging.getLogger()


class PostManager(FlagManagerMixin, TrendingManagerMixin, ViewManagerMixin, ManagerBase):

    item_type = 'post'

    def __init__(self, clients, managers=None):
        super().__init__(clients, managers=managers)
        managers = managers or {}
        managers['post'] = self
        self.album_manager = managers.get('album') or models.AlbumManager(clients, managers=managers)
        self.block_manager = managers.get('block') or models.BlockManager(clients, managers=managers)
        self.comment_manager = managers.get('comment') or models.CommentManager(clients, managers=managers)
        self.follower_manager = managers.get('follower') or models.FollowerManager(clients, managers=managers)
        self.like_manager = managers.get('like') or models.LikeManager(clients, managers=managers)
        self.user_manager = managers.get('user') or models.UserManager(clients, managers=managers)

        self.clients = clients
        if 'appsync' in clients:
            self.appsync = PostAppSync(clients['appsync'])
        if 'elasticsearch' in clients:
            self.elasticsearch_client = clients['elasticsearch']
        if 'dynamo' in clients:
            self.dynamo = PostDynamo(clients['dynamo'])
            self.image_dynamo = PostImageDynamo(clients['dynamo'])
            self.original_metadata_dynamo = PostOriginalMetadataDynamo(clients['dynamo'])

    def get_model(self, item_id, strongly_consistent=False):
        return self.get_post(item_id, strongly_consistent=strongly_consistent)

    def get_post(self, post_id, strongly_consistent=False):
        post_item = self.dynamo.get_post(post_id, strongly_consistent=strongly_consistent)
        return self.init_post(post_item) if post_item else None

    def init_post(self, post_item):
        kwargs = {
            'post_appsync': getattr(self, 'appsync', None),
            'post_dynamo': getattr(self, 'dynamo', None),
            'post_image_dynamo': getattr(self, 'image_dynamo', None),
            'post_original_metadata_dynamo': getattr(self, 'original_metadata_dynamo', None),
            'flag_dynamo': getattr(self, 'flag_dynamo', None),
            'trending_dynamo': getattr(self, 'trending_dynamo', None),
            'view_dynamo': getattr(self, 'view_dynamo', None),
            'cloudfront_client': self.clients.get('cloudfront'),
            'mediaconvert_client': self.clients.get('mediaconvert'),
            'post_verification_client': self.clients.get('post_verification'),
            's3_uploads_client': self.clients.get('s3_uploads'),
            'album_manager': self.album_manager,
            'block_manager': self.block_manager,
            'comment_manager': self.comment_manager,
            'follower_manager': self.follower_manager,
            'like_manager': self.like_manager,
            'post_manager': self,
            'user_manager': self.user_manager,
        }
        return Post(post_item, **kwargs) if post_item else None

    def add_post(
        self,
        posted_by_user,
        post_id,
        post_type,
        image_input=None,
        text=None,
        lifetime_duration=None,
        album_id=None,
        comments_disabled=None,
        likes_disabled=None,
        sharing_disabled=None,
        verification_hidden=None,
        keywords=None,
        set_as_user_photo=None,
        now=None,
    ):
        now = now or pendulum.now('utc')
        text = None if text == '' else text  # treat empty string as equivalent of null

        if post_type == PostType.TEXT_ONLY:
            if not text:
                raise PostException('Cannot add text-only post without text')
            if image_input:
                raise PostException('Cannot add text-only post with ImageInput')
            if set_as_user_photo:
                raise PostException('Cannot add text-only post with setAsUserPhoto')

        elif post_type == PostType.VIDEO:
            if image_input:
                raise PostException('Cannot add video post with ImageInput')
            if set_as_user_photo:
                raise PostException('Cannot add video post with setAsUserPhoto')

        elif post_type == PostType.IMAGE:
            if image_input and (crop := image_input.get('crop')):
                for pt, coord in itertools.product(('upperLeft', 'lowerRight'), ('x', 'y')):
                    if crop[pt][coord] < 0:
                        raise PostException(f'Image crop {pt}.{coord} cannot be negative')
                for coord in ('x', 'y'):
                    if crop['upperLeft'][coord] >= crop['lowerRight'][coord]:
                        raise PostException(
                            f'Image crop lowerRight.{coord} must be strictly greater than upperLeft.{coord}',
                        )
        else:
            raise Exception(f'Invalid PostType `{post_type}`')

        expires_at = now + lifetime_duration if lifetime_duration is not None else None
        if expires_at and expires_at <= now:
            msg = f'Refusing to add post `{post_id}` for user `{posted_by_user.id}` with non-positive lifetime'
            raise PostException(msg)

        text_tags = self.user_manager.get_text_tags(text) if text is not None else None

        # pull in user-level defaults for settings as needed
        if comments_disabled is None:
            comments_disabled = posted_by_user.item.get('commentsDisabled')
        if likes_disabled is None:
            likes_disabled = posted_by_user.item.get('likesDisabled')
        if sharing_disabled is None:
            sharing_disabled = posted_by_user.item.get('sharingDisabled')
        if verification_hidden is None:
            verification_hidden = posted_by_user.item.get('verificationHidden')

        # if an album is specified, verify it exists and is ours
        if album_id:
            album = self.album_manager.get_album(album_id)
            if not album:
                raise PostException(f'Album `{album_id}` does not exist')
            if album.user_id != posted_by_user.id:
                msg = f'Album `{album_id}` does not belong to caller user `{posted_by_user.id}`'
                raise PostException(msg)

        # add the pending post & media to dynamo in a transaction
        post_item = self.dynamo.add_pending_post(
            posted_by_user.id,
            post_id,
            post_type,
            posted_at=now,
            expires_at=expires_at,
            text=text,
            text_tags=text_tags,
            comments_disabled=comments_disabled,
            likes_disabled=likes_disabled,
            sharing_disabled=sharing_disabled,
            verification_hidden=verification_hidden,
            album_id=album_id,
            keywords=keywords,
            set_as_user_photo=set_as_user_photo,
        )
        post = self.init_post(post_item)

        # text-only posts can be completed immediately
        if post.type == PostType.TEXT_ONLY:
            post.complete(now=now)

        if post.type == PostType.IMAGE and image_input:
            # 'image_input' is straight from graphql, format dictated by schema
            image_attributes = {
                'crop': image_input.get('crop'),
                'image_format': image_input.get('imageFormat'),
                'original_format': image_input.get('originalFormat'),
                'taken_in_real': image_input.get('takenInReal'),
            }
            post._image_item = self.image_dynamo.set_initial_attributes(post_id, **image_attributes)

            if original_metadata := image_input.get('originalMetadata'):
                self.original_metadata_dynamo.add(post_id, original_metadata)

            # if the upload included the image data, complete the post immediately
            if image_data := image_input.get('imageData'):
                try:
                    post.process_image_upload(image_data=image_data, now=now)
                except Exception as err:
                    post.error(str(err))
                    if not isinstance(err, PostException):
                        raise err
                    logger.warning(str(err))

        return post

    def record_views(self, post_ids, user_id, viewed_at=None, view_type=None):
        grouped_post_ids = dict(collections.Counter(post_ids))
        if not grouped_post_ids:
            return

        results = []
        for post_id, view_count in grouped_post_ids.items():
            post = self.get_post(post_id)
            if not post:
                logger.warning(f'Cannot record view(s) by user `{user_id}` on DNE post `{post_id}`')
                continue
            results.append(post.record_view_count(user_id, view_count, viewed_at=viewed_at, view_type=view_type))

        if any(results):
            self.user_manager.dynamo.update_last_post_view_at(user_id, now=viewed_at, view_type=view_type)

    def delete_recently_expired_posts(self, now=None):
        "Delete posts that expired yesterday or today"
        now = now or pendulum.now('utc')
        yesterday = now - pendulum.duration(days=1)

        # Every run we operate on all posts that expired yesterday, and any that have expired so far today.
        # Techinically we only need to operate on yesterday's posts on today's first run,
        # but in the interest of avoiding any 'left behind' posts we do it every time.

        yesterdays_post_pks = self.dynamo.generate_expired_post_pks_by_day(yesterday.date())
        todays_post_pks = self.dynamo.generate_expired_post_pks_by_day(now.date(), now.time())

        # scan for expired posts
        for post_pk in itertools.chain(yesterdays_post_pks, todays_post_pks):
            post_item = self.dynamo.client.get_item(post_pk)
            user_item = self.user_manager.dynamo.get_user(post_item['postedByUserId'])
            logger.warning(
                f'Deleting expired post with pk ({post_pk["partitionKey"]}, {post_pk["sortKey"]}):'
                + f', posted by `{user_item["username"]}`'
                + f', posted at `{post_item.get("postedAt")}`'
                + f', with text `{post_item.get("text")}`'
                + f', with status `{post_item.get("postStatus")}`'
                + f', expired at `{post_item.get("expiresAt")}`'
            )
            self.init_post(post_item).delete()

    def delete_older_expired_posts(self, now=None):
        "Delete posts that expired yesterday or earlier, via full table scan"
        now = now or pendulum.now('utc')
        today = now.date()

        # scan for expired posts
        for post_pk in self.dynamo.generate_expired_post_pks_with_scan(today):  # excludes today
            logger.warning(f'Deleting expired post with pk ({post_pk["partitionKey"]}, {post_pk["sortKey"]})')
            post_item = self.dynamo.client.get_item(post_pk)
            self.init_post(post_item).delete()

    def find_posts(self, keywords, limit, next_token):
        query = {
            'from': next_token,
            'size': limit,
            'query': {
                'bool': {
                    'should': [
                        {'match_bool_prefix': {'keywords': {'query': keywords, 'boost': 2}}},
                        {'match': {'keywords': {'query': keywords, 'boost': 2}}},
                    ],
                }
            },
        }
        search_result = self.elasticsearch_client.query_posts(query)
        post_id_to_trending_score = {}
        sorted_post_ids = []

        for hit in search_result['hits']['hits']:
            source = hit.get('_source')
            if source is not None:
                post_id = source['postId']
                trending_score = self.get_post(source['postId']).trending_score
                post_id_to_trending_score[post_id] = trending_score

        if post_id_to_trending_score:
            # sort post ids by trending weight
            sorted_post_ids = sorted(post_id_to_trending_score, key=post_id_to_trending_score.get, reverse=True)

            available_total = search_result['hits']['total']['value']
            next_covers_to = len(search_result['hits']['hits']) + int(next_token)

            if available_total > next_covers_to:
                next_token = next_covers_to

        return {
            'nextToken': str(next_token),
            'items': sorted_post_ids,
        }

    def search_keywords(self, keyword):
        query = {
            'size': 20,
            'query': {
                'bool': {
                    'should': [
                        {'match_bool_prefix': {'keyword': {'query': keyword, 'boost': 2}}},
                        {'match': {'keyword': {'query': keyword, 'boost': 2}}},
                    ],
                }
            },
        }
        search_result = self.elasticsearch_client.query_keywords(query)
        keywords = []

        for hit in search_result['hits']['hits']:
            source = hit.get('_source')
            if source is not None:
                keywords.append(source['keyword'])

        return list(set(keywords))

    def on_user_delete_delete_all_by_user(self, user_id, old_item):
        for post_item in self.dynamo.generate_posts_by_user(user_id):
            self.init_post(post_item).delete()

    def on_flag_add(self, post_id, new_item):
        post_item = self.dynamo.increment_flag_count(post_id)
        post = self.init_post(post_item)

        user_id = new_item['sortKey'].split('/')[1]
        flagger = self.user_manager.get_user(user_id)

        # force archive the post?
        if flagger.username in self.flag_admin_usernames or post.is_crowdsourced_forced_removal_criteria_met():
            logger.warning(f'Force archiving post `{post_id}` from flagging')
            post.archive(forced=True)

    def on_comment_add(self, comment_id, new_item):
        comment = self.comment_manager.init_comment(new_item)
        by_post_owner = comment.user_id == comment.post.user_id
        self.dynamo.increment_comment_count(comment.post_id, viewed=by_post_owner)
        if not by_post_owner:
            self.dynamo.set_last_unviewed_comment_at(comment.post.item, comment.created_at)

    def on_comment_delete(self, comment_id, old_item):
        comment = self.comment_manager.init_comment(old_item)
        self.dynamo.decrement_comment_count(comment.post_id)

        if comment.post and comment.user_id != comment.post.user_id:
            # has the post owner 'viewed' that comment via reporting a view on the post?
            post_view_item = self.view_dynamo.get_view(comment.post_id, comment.post.user_id)
            post_last_viewed_at = pendulum.parse(post_view_item['lastViewedAt']) if post_view_item else None
            if not (post_last_viewed_at and post_last_viewed_at > comment.created_at):
                post_item = self.dynamo.decrement_comments_unviewed_count(comment.post_id)
                # if the comment unviewed count hit zero, then remove post from 'posts with unviewed comments' index
                if post_item and post_item.get('commentsUnviewedCount', 0) == 0:
                    self.dynamo.set_last_unviewed_comment_at(post_item, None)

    def on_like_add(self, post_id, new_item):
        like_status = new_item['likeStatus']
        if like_status == LikeStatus.ONYMOUSLY_LIKED:
            incrementor = self.dynamo.increment_onymous_like_count
        elif like_status == LikeStatus.ANONYMOUSLY_LIKED:
            incrementor = self.dynamo.increment_anonymous_like_count
        else:
            raise Exception(f'Unrecognized like status `{like_status}`')
        incrementor(post_id)

    def on_like_delete(self, post_id, old_item):
        like_status = old_item['likeStatus']
        if like_status == LikeStatus.ONYMOUSLY_LIKED:
            decrementor = self.dynamo.decrement_onymous_like_count
        elif like_status == LikeStatus.ANONYMOUSLY_LIKED:
            decrementor = self.dynamo.decrement_anonymous_like_count
        else:
            raise Exception(f'Unrecognized like status `{like_status}`')
        decrementor(post_id)

    def on_post_view_count_change_update_counts(self, post_id, new_item, old_item=None):
        if new_item.get('viewCount', 0) <= (old_item or {}).get('viewCount', 0):
            return  # view count did not increase

        _, viewed_by_user_id = new_item['sortKey'].split('/')
        post = self.get_post(post_id)
        if not post or post.user_id != viewed_by_user_id:
            return  # not viewed by post owner

        try:
            self.dynamo.clear_comments_unviewed_count(post.id)
            self.dynamo.set_last_unviewed_comment_at(post.item, None)
        except self.dynamo.client.exceptions.ConditionalCheckFailedException:
            # Race condition: the post was deleted.
            # Make sure that's the case before swallowing the exception.
            if post.refresh_item().item:
                raise

    def on_album_delete_remove_posts(self, album_id, old_item):
        for post_id in self.dynamo.generate_post_ids_in_album(album_id):
            if post := self.get_post(post_id):
                post.set_album(None)

    def on_post_status_change_fire_gql_notifications(self, post_id, new_item, old_item):
        old_post = self.init_post(old_item)
        new_post = self.init_post(new_item)
        kwargs = {'postId': post_id}

        if new_post.status == PostStatus.ERROR:
            self.appsync.client.fire_notification(new_post.user_id, GqlNotificationType.POST_ERROR, **kwargs)

        initial_statuses = (PostStatus.PENDING, PostStatus.PROCESSING)
        if new_post.status == PostStatus.COMPLETED and old_post.status in initial_statuses:
            self.appsync.client.fire_notification(new_post.user_id, GqlNotificationType.POST_COMPLETED, **kwargs)

    def on_post_verification_hidden_change_update_is_verified(self, post_id, new_item, old_item=None):
        old_verif_hidden = (old_item or {}).get('verificationHidden', False)
        new_verif_hidden = new_item.get('verificationHidden', False)

        is_verif = None
        if old_verif_hidden is False and new_verif_hidden is True:
            is_verif = new_item.get('isVerified')
        if old_verif_hidden is True and new_verif_hidden is False:
            is_verif = new_item.get('isVerifiedHiddenValue')

        if is_verif is not None:
            self.dynamo.set_is_verified(post_id, is_verif, hidden=new_verif_hidden)

    def on_post_view_add_delete_sync_viewed_by_counts(self, post_id, new_item=None, old_item=None):
        assert not (new_item and old_item), 'Should only be called for INSERT and REMOVE'
        user_id = (new_item or old_item)['sortKey'].split('/')[1]
        post = self.get_post(post_id)

        # ignore posts that have been deleted and our own views on our own post
        if not post or post.user_id == user_id:
            return

        if new_item:
            self.dynamo.increment_viewed_by_count(post_id)
            self.user_manager.dynamo.increment_post_viewed_by_count(post.user_id)
        if old_item:
            self.dynamo.decrement_viewed_by_count(post_id)
            self.user_manager.dynamo.decrement_post_viewed_by_count(post.user_id)

    def on_post_view_change_update_trending(self, post_id, new_item, old_item=None):
        # only COMPLETED posts should exist in trending
        post = self.get_post(post_id)
        if not post or post.status != PostStatus.COMPLETED:
            return

        # a user's views of their own post don't earning trending points
        user_id = new_item['sortKey'].split('/')[1]
        if post.user_id == user_id:
            return

        new_focus_view_count = new_item.get('focusViewCount', 0)
        old_focus_view_count = (old_item or {}).get('focusViewCount', 0)

        new_view_count = new_item.get('viewCount', 0)
        old_view_count = (old_item or {}).get('viewCount', 0)

        new_thumbnail_view_count = new_view_count - new_focus_view_count
        old_thumbnail_view_count = old_view_count - old_focus_view_count

        now = pendulum.parse(new_item['lastViewedAt'])
        all_trending_kwargs = []
        if new_focus_view_count > 0 and old_focus_view_count == 0:
            all_trending_kwargs.append({'now': now, 'multiplier': post.get_trending_multiplier(ViewType.FOCUS)})
        if new_thumbnail_view_count > 0 and old_thumbnail_view_count == 0:
            all_trending_kwargs.append({'now': now, 'multiplier': post.get_trending_multiplier()})

        for trending_kwargs in all_trending_kwargs:
            recorded = post.trending_increment_score(**trending_kwargs)
            if recorded:
                post.user.trending_increment_score(**trending_kwargs)

    def on_post_delete(self, post_id, old_item):
        self.elasticsearch_client.delete_post(post_id)
        # remove old keywords
        keywords = old_item.get('keywords', [])
        for k in keywords:
            self.elasticsearch_client.delete_keyword(post_id, k)

    def sync_elasticsearch(self, post_id, new_item, old_item=None):
        self.elasticsearch_client.put_post(post_id, new_item['keywords'])
        # remove old keywords
        if old_item is not None and old_item['keywords'] is not None:
            for k in old_item['keywords']:
                self.elasticsearch_client.delete_keyword(post_id, k)
        # add new keywords
        keywords = new_item.get('keywords', [])
        for k in keywords:
            self.elasticsearch_client.put_keyword(post_id, k)
