import base64
import io
import logging

import colorthief
import pendulum
import PIL.Image

from app.mixins.flag.model import FlagModelMixin
from app.mixins.trending.model import TrendingModelMixin
from app.mixins.view.enums import ViewType
from app.mixins.view.model import ViewModelMixin
from app.models.follower.enums import FollowStatus
from app.models.user.enums import UserPrivacyStatus, UserSubscriptionLevel
from app.models.user.exceptions import UserException
from app.utils import image_size

from .cached_image import CachedImage
from .enums import PostNotificationType, PostStatus, PostType
from .exceptions import PostException
from .text_image import generate_text_image

logger = logging.getLogger()

# keep in sync with object created handlers defined serverless.yml
VIDEO_ORIGINAL_FILENAME = 'video-original.mov'
VIDEO_HLS_PREFIX = 'video-hls/video'
VIDEO_POSTER_PREFIX = 'video-poster/poster'
IMAGE_DIR = 'image'


class ColorThiefFromImage(colorthief.ColorThief):
    def __init__(self, image):
        self.image = image


class Post(FlagModelMixin, TrendingModelMixin, ViewModelMixin):

    item_type = 'post'

    def __init__(
        self,
        item,
        post_appsync=None,
        post_dynamo=None,
        post_image_dynamo=None,
        post_original_metadata_dynamo=None,
        cloudfront_client=None,
        mediaconvert_client=None,
        post_verification_client=None,
        s3_uploads_client=None,
        album_manager=None,
        block_manager=None,
        comment_manager=None,
        follower_manager=None,
        like_manager=None,
        post_manager=None,
        user_manager=None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if post_appsync is not None:
            self.appsync = post_appsync
        if post_dynamo is not None:
            self.dynamo = post_dynamo
        if post_image_dynamo is not None:
            self.image_dynamo = post_image_dynamo
        if post_original_metadata_dynamo is not None:
            self.original_metadata_dynamo = post_original_metadata_dynamo

        if cloudfront_client is not None:
            self.cloudfront_client = cloudfront_client
        if mediaconvert_client is not None:
            self.mediaconvert_client = mediaconvert_client
        if post_verification_client is not None:
            self.post_verification_client = post_verification_client
        if s3_uploads_client is not None:
            self.s3_uploads_client = s3_uploads_client

        if album_manager is not None:
            self.album_manager = album_manager
        if block_manager is not None:
            self.block_manager = block_manager
        if comment_manager is not None:
            self.comment_manager = comment_manager
        if follower_manager is not None:
            self.follower_manager = follower_manager
        if like_manager is not None:
            self.like_manager = like_manager
        if post_manager is not None:
            self.post_manager = post_manager
        if user_manager is not None:
            self.user_manager = user_manager

        self.item = item
        # immutables
        self.id = item['postId']
        self.type = self.item['postType']
        self.user_id = item['postedByUserId']

        # lazy caches
        if self.type == PostType.TEXT_ONLY:
            text = self.item['text']
            self.k4_jpeg_cache = CachedImage(
                self.id, source=lambda: generate_text_image(text, image_size.K4.max_dimensions)
            )
            self.p1080_jpeg_cache = CachedImage(
                self.id, source=lambda: generate_text_image(text, image_size.P1080.max_dimensions)
            )
        elif s3_uploads_client:
            self.native_heic_cache = CachedImage(
                self.id,
                image_size=image_size.NATIVE_HEIC,
                s3_client=s3_uploads_client,
                s3_path=self.get_image_path(image_size.NATIVE_HEIC),
            )
            self.native_jpeg_cache = CachedImage(
                self.id,
                image_size=image_size.NATIVE,
                s3_client=s3_uploads_client,
                s3_path=self.get_image_path(image_size.NATIVE),
            )
            self.k4_jpeg_cache = CachedImage(
                self.id,
                image_size=image_size.K4,
                s3_client=s3_uploads_client,
                s3_path=self.get_image_path(image_size.K4),
            )
            self.p1080_jpeg_cache = CachedImage(
                self.id,
                image_size=image_size.P1080,
                s3_client=s3_uploads_client,
                s3_path=self.get_image_path(image_size.P1080),
            )
            self.p480_jpeg_cache = CachedImage(
                self.id,
                image_size=image_size.P480,
                s3_client=s3_uploads_client,
                s3_path=self.get_image_path(image_size.P480),
            )
            self.p64_jpeg_cache = CachedImage(
                self.id,
                image_size=image_size.P64,
                s3_client=s3_uploads_client,
                s3_path=self.get_image_path(image_size.P64),
            )

    @property
    def status(self):
        return self.item['postStatus']

    @property
    def posted_at(self):
        return pendulum.parse(self.item['postedAt'])

    @property
    def s3_prefix(self):
        return '/'.join([self.user_id, 'post', self.id])

    @property
    def image_item(self):
        this = self if hasattr(self, '_image_item') else self.refresh_image_item()
        return this._image_item

    @property
    def user(self):
        if not hasattr(self, '_user'):
            self._user = self.user_manager.get_user(self.user_id)
        return self._user

    @property
    def is_verified(self):
        return self.item.get('isVerified')

    @property
    def original_post_id(self):
        return self.item.get('originalPostId', self.id)

    @property
    def viewed_by_count(self):
        return self.item.get('viewedByCount', 0)

    @property
    def trending_score(self):
        return super().trending_score if super().trending_score else 0

    def refresh_item(self, strongly_consistent=False):
        self.item = self.dynamo.get_post(self.id, strongly_consistent=strongly_consistent)
        return self

    def refresh_image_item(self, strongly_consistent=False):
        self._image_item = self.image_dynamo.get(self.id, strongly_consistent=strongly_consistent) or {}
        return self

    def get_s3_image_path(self, size):
        "From within the user's directory, return the path to the s3 object of the requested size"
        return '/'.join([self.item['postedByUserId'], 'post', self.item['postId'], 'image', size.filename])

    def get_original_video_path(self):
        return f'{self.s3_prefix}/{VIDEO_ORIGINAL_FILENAME}'

    def get_poster_video_path_prefix(self):
        return f'{self.s3_prefix}/{VIDEO_POSTER_PREFIX}'

    def get_poster_path(self):
        return f'{self.s3_prefix}/{VIDEO_POSTER_PREFIX}.0000000.jpg'

    def get_image_path(self, size):
        return f'{self.s3_prefix}/{IMAGE_DIR}/{size.filename}'

    def get_hls_video_path_prefix(self):
        return f'{self.s3_prefix}/{VIDEO_HLS_PREFIX}'

    def get_hls_master_m3u8_url(self):
        path = f'{self.s3_prefix}/{VIDEO_HLS_PREFIX}.m3u8'
        return self.cloudfront_client.generate_unsigned_url(path)

    def get_hls_access_cookies(self):
        s3_path = self.get_hls_video_path_prefix()
        signature_path = s3_path + '*'
        cookie_path = '/' + '/'.join(s3_path.split('/')[:-1]) + '/'  # remove trailing partial filename
        cookies = self.cloudfront_client.generate_presigned_cookies(signature_path)
        return {
            'domain': self.cloudfront_client.domain,
            'path': cookie_path,
            'expiresAt': cookies['ExpiresAt'],
            'policy': cookies['CloudFront-Policy'],
            'signature': cookies['CloudFront-Signature'],
            'keyPairId': cookies['CloudFront-Key-Pair-Id'],
        }

    def get_video_writeonly_url(self):
        path = self.get_original_video_path()
        return self.cloudfront_client.generate_presigned_url(path, ['PUT'])

    def get_image_readonly_url(self, size):
        path = self.get_image_path(size)
        return self.cloudfront_client.generate_presigned_url(path, ['GET', 'HEAD'])

    def get_image_writeonly_url(self):
        assert self.type == PostType.IMAGE
        size = image_size.NATIVE_HEIC if self.image_item.get('imageFormat') == 'HEIC' else image_size.NATIVE
        path = self.get_image_path(size)
        return self.cloudfront_client.generate_presigned_url(path, ['PUT'])

    def serialize(self, caller_user_id):
        resp = self.item.copy()
        resp['postedBy'] = self.user_manager.get_user(self.user_id).serialize(caller_user_id)
        return resp

    def build_image_thumbnails(self):
        image = self.native_jpeg_cache.readonly_image.copy()
        # ordered by decreasing size
        for cache in (self.k4_jpeg_cache, self.p1080_jpeg_cache, self.p480_jpeg_cache, self.p64_jpeg_cache):
            try:
                image.thumbnail(cache.image_size.max_dimensions, resample=PIL.Image.LANCZOS)
            except Exception as err:
                raise PostException(f'Unable to thumbnail image as jpeg for post `{self.id}`: {err}') from err
            cache.set_image(image)
            cache.flush()

    def process_image_upload(self, image_data=None, now=None):
        assert self.type == PostType.IMAGE, 'Can only process_image_upload() for IMAGE posts'
        assert self.status in (
            PostStatus.PENDING,
            PostStatus.ERROR,
        ), 'Can only process_image_upload() for PENDING & ERROR posts'
        now = now or pendulum.now('utc')

        # mark ourselves as processing
        self.item = self.dynamo.set_post_status(self.item, PostStatus.PROCESSING)

        # set up a cached image with the raw data (four different ways to receive the data now)
        source_cached_image = (
            self.native_heic_cache if self.image_item.get('imageFormat') == 'HEIC' else self.native_jpeg_cache
        )
        if image_data:
            source_cached_image.set_data(io.BytesIO(base64.b64decode(image_data)))

        if crop := self.image_item.get('crop'):
            source_cached_image.crop(crop)

        if source_cached_image != self.native_jpeg_cache:
            self.native_jpeg_cache.set_image(source_cached_image.readonly_image)  # set_image makes a copy

        if self.native_jpeg_cache.is_synced is False:
            self.native_jpeg_cache.flush()

        if self.native_heic_cache.is_synced is False:
            # the HEIC image was edited (cropped) but we can't save that as HEIC, so we just delete it
            self.native_heic_cache.clear()
            self.native_heic_cache.flush(include_deletes=True)

        self.build_image_thumbnails()
        self.set_height_and_width()
        self.set_colors()
        self.set_is_verified()
        self.set_checksum()
        self.complete(now=now)

    def start_processing_video_upload(self):
        assert self.type == PostType.VIDEO, 'Can only process_video_upload() for VIDEO posts'
        assert self.status in (PostStatus.PENDING, PostStatus.ERROR), 'Can only call for PENDING & ERROR posts'

        # mark ourselves as processing
        self.item = self.dynamo.set_post_status(self.item, PostStatus.PROCESSING)

        # start the media convert job
        input_key = self.get_original_video_path()
        video_output_key_prefix = self.get_hls_video_path_prefix()
        image_output_key_prefix = self.get_poster_video_path_prefix()
        self.mediaconvert_client.create_job(input_key, video_output_key_prefix, image_output_key_prefix)

    def finish_processing_video_upload(self):
        assert self.type == PostType.VIDEO, 'Can only process_video_upload() for VIDEO posts'
        assert self.status == PostStatus.PROCESSING, 'Can only call for PROCESSING posts'

        # make the poster image our new 'native' image
        poster_path = self.get_poster_path()
        native_path = self.get_image_path(image_size.NATIVE)
        self.s3_uploads_client.copy_object(poster_path, native_path)
        self.s3_uploads_client.delete_object(poster_path)

        self.build_image_thumbnails()
        self.complete()

    def error(self, reason):
        if self.status not in (PostStatus.PENDING, PostStatus.PROCESSING):
            raise PostException('Only posts with status PENDING or PROCESSING may transition to ERROR')
        self.item = self.dynamo.set_post_status(self.item, PostStatus.ERROR, status_reason=reason)
        return self

    def complete(self, now=None):
        "Transition the post to COMPLETED status"
        now = now or pendulum.now('utc')

        if self.status in (PostStatus.COMPLETED, PostStatus.ARCHIVED, PostStatus.DELETING):
            msg = f'Refusing to change post `{self.id}` with status `{self.status}` to `{PostStatus.COMPLETED}`'
            raise PostException(msg)

        # Determine the original_post_id, if this post isn't original
        original_post_id = None
        if self.type == PostType.IMAGE:
            # need strongly consistent because checksum may have been just set
            checksum = self.refresh_item(strongly_consistent=True).item['checksum']
            post_id = self.dynamo.get_first_with_checksum(checksum)
            if post_id and post_id != self.id:
                original_post_id = post_id
        set_as_user_photo = self.item.get('setAsUserPhoto')

        album_id = self.item.get('albumId')
        album = self.album_manager.get_album(album_id) if album_id else None
        album = album.increment_rank_count() if album else None
        if album_id and not album:
            # album has disappeared, so remove the post from the album
            self.item = self.dynamo.set_album_id(self.item, None)
        album_rank = album.get_last_rank() if album else None

        # complete the post
        self.item = self.dynamo.set_post_status(
            self.item,
            PostStatus.COMPLETED,
            original_post_id=original_post_id,
            album_rank=album_rank,
        )

        # update the user's profile photo, if needed
        if set_as_user_photo:
            try:
                self.user.update_photo(self.id)
            except UserException as err:
                logger.warning(f'Unable to set user photo with post `{self.id}`: {err}')

        # update the first story if needed
        if self.item.get('expiresAt'):
            self.follower_manager.refresh_first_story(story_now=self.item)

        # give new posts a free bump into trending, but not their user
        trending_kwargs = {'now': now, 'multiplier': self.get_trending_multiplier()}
        self.trending_increment_score(**trending_kwargs)

        # alert frontend
        self.appsync.trigger_notification(PostNotificationType.COMPLETED, self)

        return self

    def archive(self, forced=False):
        "Transition the post to ARCHIVED status"
        if self.status != PostStatus.COMPLETED:
            raise PostException(f'Cannot archive post with status `{self.status}`')

        # set the post as archived
        self.item = self.dynamo.set_post_status(self.item, PostStatus.ARCHIVED)

        if forced:
            self.user_manager.dynamo.increment_post_forced_archiving_count(self.user_id)

        # dislike all likes of the post
        self.like_manager.dislike_all_of_post(self.id)

        # if it was the first followed story, refresh that
        if self.item.get('expiresAt'):
            self.follower_manager.refresh_first_story(story_prev=self.item)

        # delete the trending index, if it exists
        self.trending_delete()

        return self

    def restore(self):
        "Transition the post out of ARCHIVED status"
        if self.status != PostStatus.ARCHIVED:
            raise PostException(f'Post `{self.id}` is not archived (has status `{self.status}`)')

        album_id = self.item.get('albumId')
        album = self.album_manager.get_album(album_id) if album_id else None
        album = album.increment_rank_count() if album else None
        if album_id and not album:
            # album has disappeared, so remove the post from the album
            self.item = self.dynamo.set_album_id(self.item, None)
        album_rank = album.get_last_rank() if album else None

        # restore the post
        self.item = self.dynamo.set_post_status(self.item, PostStatus.COMPLETED, album_rank=album_rank)

        # refresh the first story if needed
        if self.item.get('expiresAt'):
            self.follower_manager.refresh_first_story(story_now=self.item)

        return self

    def delete(self):
        "Delete the post and all its media"
        # mark the post and the media as in the deleting process
        self.item = self.dynamo.set_post_status(self.item, PostStatus.DELETING)

        # dislike all likes of the post
        self.like_manager.dislike_all_of_post(self.id)

        # delete all comments on the post
        self.comment_manager.delete_all_on_post(self.id)

        # if it was the first followed story, refresh that
        if self.item.get('expiresAt'):
            self.follower_manager.refresh_first_story(story_prev=self.item)

        # delete the trending index, if it exists
        self.trending_delete()

        # do the deletes for real
        self.s3_uploads_client.delete_objects_with_prefix(self.s3_prefix)
        if self.image_item:
            self.image_dynamo.delete(self.id)
        self.original_metadata_dynamo.delete(self.id)
        self.dynamo.delete_post(self.id)

        return self

    def set(
        self,
        text=None,
        comments_disabled=None,
        likes_disabled=None,
        sharing_disabled=None,
        verification_hidden=None,
        keywords=None,
    ):
        args = [text, comments_disabled, likes_disabled, sharing_disabled, verification_hidden, keywords]
        if all(v is None for v in args):
            raise PostException('Empty edit requested')

        if self.type == PostType.TEXT_ONLY and text == '':
            raise PostException('Cannot set text to null on text-only post')

        text_tags = self.user_manager.get_text_tags(text) if text is not None else None
        self.item = self.dynamo.set(
            self.id,
            text=text,
            text_tags=text_tags,
            comments_disabled=comments_disabled,
            likes_disabled=likes_disabled,
            sharing_disabled=sharing_disabled,
            verification_hidden=verification_hidden,
            keywords=keywords,
        )
        return self

    def set_height_and_width(self):
        width, height = self.native_jpeg_cache.readonly_image.size
        self._image_item = self.image_dynamo.set_height_and_width(self.id, height, width)
        return self

    def set_colors(self):
        try:
            colors = ColorThiefFromImage(self.native_jpeg_cache.readonly_image).get_palette(color_count=5)
        except Exception as err:
            logger.warning(f'ColorTheif failed to get palette with error `{err}` for post `{self.id}`')
        else:
            self._image_item = self.image_dynamo.set_colors(self.id, colors)
        return self

    def set_checksum(self):
        path = self.get_image_path(image_size.NATIVE)
        checksum = self.s3_uploads_client.get_object_checksum(path)
        self.item = self.dynamo.set_checksum(self.id, self.item['postedAt'], checksum)
        return self

    def set_is_verified(self):
        path = self.get_image_path(image_size.NATIVE)
        image_url = self.cloudfront_client.generate_presigned_url(path, ['GET', 'HEAD'])
        is_verified = self.post_verification_client.verify_image(
            image_url,
            image_format=self.image_item.get('imageFormat'),
            original_format=self.image_item.get('originalFormat'),
            taken_in_real=self.image_item.get('takenInReal'),
        )
        hidden = self.item.get('verificationHidden', False)
        self.item = self.dynamo.set_is_verified(self.id, is_verified, hidden=hidden)
        return self

    def set_expires_at(self, expires_at):
        prev_item = self.item.copy() if 'expiresAt' in self.item else None
        if expires_at:
            self.item = self.dynamo.set_expires_at(self.item, expires_at)
        else:
            self.item = self.dynamo.remove_expires_at(self.id)
        now_item = self.item.copy() if 'expiresAt' in self.item else None
        if prev_item or now_item:
            self.follower_manager.refresh_first_story(story_prev=prev_item, story_now=now_item)
        return self

    def set_album(self, album_id):
        "Set the album the post is in. Set album_id to None to remove the post from all albums."
        prev_album_id = self.item.get('albumId')

        if prev_album_id == album_id:
            return self

        # if an album is specified, verify it exists and is ours
        album_rank = None
        album = self.album_manager.get_album(album_id) if album_id else None
        if album:
            if album.user_id != self.user_id:
                raise PostException(f'Album `{album_id}` and post `{self.id}` belong to different users')
            if self.status == PostStatus.COMPLETED:
                album = album.increment_rank_count()
                album_rank = album.get_last_rank() if album else None
        if album_id and not album:
            raise PostException(f'Album `{album_id}` does not exist')

        self.item = self.dynamo.set_album_id(self.item, album_id, album_rank=album_rank)
        return self

    def set_album_order(self, preceding_post_id):
        album_id = self.item.get('albumId')
        if not album_id:
            raise PostException(f'Post `{self.id}` is not in an album')

        preceding_post = None
        if preceding_post_id:
            preceding_post = self.post_manager.get_post(preceding_post_id)
            if not preceding_post:
                raise PostException(f'Preceding post `{preceding_post_id}` does not exist')
            if preceding_post.user_id != self.user_id:
                raise PostException(f'Preceding post `{preceding_post_id}` does not belong to caller')
            if preceding_post.item.get('albumId') != album_id:
                raise PostException(f'Preceding post `{preceding_post_id}` is not in album post is in')

        before_rank = preceding_post.item['gsiK3SortKey'] if preceding_post else None
        after_post_id = next(self.dynamo.generate_post_ids_in_album(album_id, after_rank=before_rank), None)
        if after_post_id == self.id:
            # we're already in that position. No-op
            return

        album = self.album_manager.get_album(album_id)
        album = album.increment_rank_count() if album else None
        if not album:
            # album has disappeared, so remove the post from the album
            self.item = self.dynamo.set_album_id(self.item, None)
            # fail with server error - api client did nothing wrong
            raise Exception(f'Album `{album_id}` that post `{self.id}` was in does not exist')

        # determine the post's new rank
        if before_rank is not None:
            if after_post_id:
                # putting the post in between two posts
                after_post = self.post_manager.get_post(after_post_id)
                after_rank = after_post.item['gsiK3SortKey']
                album_rank = (before_rank + after_rank) / 2
            else:
                # putting the post at the back
                album_rank = album.get_last_rank()
        else:
            # putting the post at the front
            album_rank = album.get_first_rank()

        self.item = self.dynamo.set_album_rank(self.id, album_rank)
        return self

    def flag(self, user):
        # if the post is from a private user then we must be a follower to flag the post
        posted_by_user = self.user_manager.get_user(self.user_id)
        if posted_by_user.item['privacyStatus'] != UserPrivacyStatus.PUBLIC:
            follow = self.follower_manager.get_follow(user.id, self.user_id)
            if not follow or follow.status != FollowStatus.FOLLOWING:
                raise PostException(f'User does not have access to post `{self.id}`')

        return super().flag(user)

    def record_view_count(self, user_id, view_count, viewed_at=None, view_type=None):
        if self.status != PostStatus.COMPLETED:
            logger.warning(f'Cannot record views by user `{user_id}` on non-COMPLETED post `{self.id}`')
            return False

        # record user's view of their own post, but don't increment any counters about it
        # their view will be filtered out when looking at Post.viewedBy
        super().record_view_count(user_id, view_count, viewed_at=viewed_at, view_type=view_type)

        # If this is a non-original post, count this like a view of the original post as well
        if self.original_post_id != self.id:
            original_post = self.post_manager.get_post(self.original_post_id)
            if original_post:
                original_post.record_view_count(user_id, view_count, viewed_at=viewed_at, view_type=view_type)

        return True

    def get_trending_multiplier(self, view_type=None):
        multiplier = 1
        if self.is_verified is False:  # note that non-image posts have is_verified value of None
            multiplier /= 2
        if self.user.subscription_level == UserSubscriptionLevel.DIAMOND:
            multiplier *= 4
        if view_type == ViewType.FOCUS:
            multiplier *= 2
        return multiplier

    def trending_increment_score(self, now=None, **kwargs):
        now = now or pendulum.now('utc')

        # keep non-original posts out of trending
        if self.type == PostType.IMAGE and self.original_post_id != self.id:
            return False

        # keep the 'real' user's posts out of trending
        if self.user_id == self.user_manager.real_user_id:
            return False

        # posts over 24 hours old don't earn more trending points
        if now - self.posted_at > pendulum.duration(hours=24):
            return False

        return super().trending_increment_score(now=now, **kwargs)
