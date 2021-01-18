class LikeException(Exception):
    pass


class AlreadyLiked(LikeException):
    def __init__(self, user_id, post_id):
        self.user_id = user_id
        self.post_id = post_id
        super().__init__()

    def __str__(self):
        return f'User `{self.user_id}` has already liked post `{self.post_id}`'


class NotLikedWithStatus(LikeException):
    def __init__(self, user_id, post_id, like_status):
        self.user_id = user_id
        self.post_id = post_id
        self.like_status = like_status
        super().__init__()

    def __str__(self):
        return f'User `{self.user_id}` has not liked post `{self.post_id}` with status `{self.like_status}`'
