import logging

logger = logging.getLogger()


class Like:
    def __init__(self, like_item, like_dynamo, post_manager=None):
        self.dynamo = like_dynamo
        if post_manager:
            self.post_manager = post_manager
        self.item = like_item
        self.liked_by_user_id = like_item['likedByUserId']
        self.post_id = like_item['postId']

    def dislike(self):
        like_status = self.item['likeStatus']
        self.dynamo.delete_like(self.liked_by_user_id, self.post_id, like_status)
