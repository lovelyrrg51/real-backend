import logging

import pendulum

from app import models

from .dynamo import AlbumDynamo
from .model import Album

logger = logging.getLogger()


class AlbumManager:

    zero_post_lifetime = pendulum.duration(hours=24)

    def __init__(self, clients, managers=None):
        managers = managers or {}
        managers['album'] = self
        self.post_manager = managers.get('post') or models.PostManager(clients, managers=managers)
        self.user_manager = managers.get('user') or models.UserManager(clients, managers=managers)

        self.clients = clients
        if 'dynamo' in clients:
            self.dynamo = AlbumDynamo(clients['dynamo'])

    def get_album(self, album_id):
        album_item = self.dynamo.get_album(album_id)
        return self.init_album(album_item) if album_item else None

    def init_album(self, album_item):
        return Album(
            album_item,
            self.dynamo,
            s3_uploads_client=self.clients.get('s3_uploads'),
            cloudfront_client=self.clients.get('cloudfront'),
            user_manager=self.user_manager,
            post_manager=self.post_manager,
        )

    def add_album(self, caller_user_id, album_id, name, description=None, now=None):
        now = now or pendulum.now('utc')
        description = None if description == '' else description  # treat empty string as null
        album_item = self.dynamo.add_album(album_id, caller_user_id, name, description, created_at=now)
        return self.init_album(album_item)

    def on_user_delete_delete_all_by_user(self, user_id, old_item):
        for album_item in self.dynamo.generate_by_user(user_id):
            self.init_album(album_item).delete()

    def garbage_collect(self, now=None):
        now = now or pendulum.now('utc')
        generator = self.dynamo.generate_keys_to_delete(now)
        return self.dynamo.client.batch_delete_items(generator)

    def on_album_delete_delete_album_art(self, album_id, old_item):
        album = self.init_album(old_item)
        prefix = album.get_art_image_path_prefix()
        album.s3_uploads_client.delete_objects_with_prefix(prefix)

    def on_album_add_edit_sync_delete_at(self, album_id, new_item, old_item=None):
        new_count = new_item.get('postCount', 0)
        if new_count == 0 and 'gsiK1PartitionKey' not in new_item:
            self.dynamo.set_delete_at(album_id, pendulum.now('utc') + self.zero_post_lifetime)
        if new_count > 0 and 'gsiK1PartitionKey' in new_item:
            self.dynamo.clear_delete_at(album_id)

    def on_album_posts_last_updated_at_change_update_art_if_needed(self, album_id, new_item, old_item=None):
        album = self.init_album(new_item)
        album.update_art_if_needed()

    def on_post_album_change_update_counts_and_timestamps(self, post_id, new_item=None, old_item=None):
        new_album_id = (new_item or {}).get('albumId')
        old_album_id = (old_item or {}).get('albumId')
        # all non-completed posts are given rank of -1
        new_album_rank = (new_item or {}).get('gsiK3SortKey', -1)
        old_album_rank = (old_item or {}).get('gsiK3SortKey', -1)

        # would be nice to match this to when the post was changed,
        # rather than when this listener gets triggered
        now = pendulum.now('utc')

        if new_album_id == old_album_id:
            if old_album_rank == -1:
                self.dynamo.increment_post_count(new_album_id, now=now)
            elif new_album_rank == -1:
                self.dynamo.decrement_post_count(new_album_id, now=now)
            else:
                self.dynamo.update_posts_last_updated_at(new_album_id, now=now)
        else:
            if new_album_rank != -1:
                self.dynamo.increment_post_count(new_album_id, now=now)
            if old_album_rank != -1:
                self.dynamo.decrement_post_count(old_album_id, now=now)
