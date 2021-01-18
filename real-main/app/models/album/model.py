import hashlib
import io
import itertools
import logging
import os

import PIL.Image

from app.utils import image_size

from . import art
from .exceptions import AlbumException

logger = logging.getLogger()

CLOUDFRONT_FRONTEND_RESOURCES_DOMAIN = os.environ.get('CLOUDFRONT_FRONTEND_RESOURCES_DOMAIN')


class Album:

    jpeg_content_type = 'image/jpeg'

    def __init__(
        self,
        album_item,
        album_dynamo,
        cloudfront_client=None,
        s3_uploads_client=None,
        user_manager=None,
        post_manager=None,
        frontend_resources_domain=CLOUDFRONT_FRONTEND_RESOURCES_DOMAIN,
    ):
        self.dynamo = album_dynamo
        if cloudfront_client:
            self.cloudfront_client = cloudfront_client
        if s3_uploads_client:
            self.s3_uploads_client = s3_uploads_client
        if post_manager:
            self.post_manager = post_manager
        if user_manager:
            self.user_manager = user_manager
        self.frontend_resources_domain = frontend_resources_domain
        self.item = album_item
        self.id = album_item['albumId']
        self.user_id = album_item['ownedByUserId']

    def refresh_item(self, strongly_consistent=False):
        self.item = self.dynamo.get_album(self.id, strongly_consistent=strongly_consistent)
        return self

    def serialize(self, caller_user_id):
        resp = self.item.copy()
        resp['ownedBy'] = self.user_manager.get_user(self.user_id).serialize(caller_user_id)
        return resp

    def update(self, name=None, description=None):
        if name == '':
            raise AlbumException('All albums must have names')
        self.item = self.dynamo.set(self.id, name=name, description=description)
        return self

    def delete(self):
        self.dynamo.delete_album(self.id)
        return self

    def get_first_rank(self):
        "Return the rank to be used for a post to appear as first in the album"
        if self.item is None or self.item.get('rankCount', 0) == 0:
            return None
        rank_spaces = self.item['rankCount'] + 1
        return 2 / rank_spaces - 1

    def get_last_rank(self):
        "Return the rank to be used for a post to appear as last in the album"
        if self.item is None or self.item.get('rankCount', 0) == 0:
            return None
        rank_spaces = self.item['rankCount'] + 1
        return 1 - 2 / rank_spaces

    def get_art_image_url(self, size):
        art_image_path = self.get_art_image_path(size)
        if art_image_path:
            return self.cloudfront_client.generate_presigned_url(art_image_path, ['GET', 'HEAD'])
        return f'https://{self.frontend_resources_domain}/default-album-art/{size.filename}'

    def get_art_image_path_prefix(self):
        return '/'.join([self.user_id, 'album', self.id])

    def get_art_image_path(self, size, art_hash=None):
        art_hash = art_hash or self.item.get('artHash')
        if not art_hash:
            return None
        return '/'.join([self.get_art_image_path_prefix(), art_hash, size.filename])

    def get_post_ids_for_art(self):
        # we only want a square number of post ids, max of 4x4
        post_ids_gen = self.post_manager.dynamo.generate_post_ids_in_album(self.id, completed=True)
        post_ids = list(itertools.islice(post_ids_gen, 16))
        if len(post_ids) < 16:
            post_ids = post_ids[:9]
        if len(post_ids) < 9:
            post_ids = post_ids[:4]
        if len(post_ids) < 4:
            post_ids = post_ids[:1]
        return post_ids

    def increment_rank_count(self):
        "Upon failure, log a WARNING and return None"
        self.item = self.dynamo.increment_rank_count(self.id)
        return self if self.item else None

    def update_art_if_needed(self):
        post_ids = self.get_post_ids_for_art()
        if post_ids:
            new_art_hash = hashlib.md5(''.join(post_ids).encode('utf-8')).hexdigest()
        else:
            new_art_hash = None

        old_art_hash = self.item.get('artHash')
        if new_art_hash == old_art_hash:
            return self  # no changes

        posts = [self.post_manager.get_post(post_id) for post_id in post_ids]
        if len(posts) == 0:
            new_native_image = None
        elif len(posts) == 1:
            new_native_image = posts[0].k4_jpeg_cache.readonly_image
        else:
            images = [post.p1080_jpeg_cache.readonly_image for post in posts]
            new_native_image = art.generate_zoomed_grid(images)

        if new_native_image:
            # convert to jpeg
            buf_out = io.BytesIO()
            new_native_image.save(buf_out, format='JPEG', quality=100)
            buf_out.seek(0)
            self.save_art_images(new_art_hash, buf_out)

        self.item = self.dynamo.set_album_art_hash(self.id, new_art_hash)

        if old_art_hash:
            self.delete_art_images(old_art_hash)

        return self

    def delete_art_images(self, art_hash):
        # remove the images from s3
        for size in image_size.JPEGS:
            path = self.get_art_image_path(size, art_hash=art_hash)
            self.s3_uploads_client.delete_object(path)

    def save_art_images(self, art_hash, native_image_buf):
        # save the native size to S3
        path = self.get_art_image_path(image_size.NATIVE, art_hash=art_hash)
        self.s3_uploads_client.put_object(path, native_image_buf.read(), self.jpeg_content_type)

        # generate and save thumbnails
        native_image_buf.seek(0)
        image = PIL.Image.open(native_image_buf)
        for size in image_size.THUMBNAILS:  # ordered by decreasing size
            image.thumbnail(size.max_dimensions, resample=PIL.Image.LANCZOS)
            in_mem_file = io.BytesIO()
            image.save(in_mem_file, format='JPEG', quality=100, icc_profile=image.info.get('icc_profile'))
            in_mem_file.seek(0)
            path = self.get_art_image_path(size, art_hash=art_hash)
            self.s3_uploads_client.put_object(path, in_mem_file.read(), self.jpeg_content_type)
