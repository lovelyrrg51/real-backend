class FollowerException(Exception):
    pass


class FollowerAlreadyExists(FollowerException):
    def __init__(self, follower_user_id, followed_user_id):
        self.follower_user_id = follower_user_id
        self.followed_user_id = followed_user_id
        super().__init__()

    def __str__(self):
        return (
            f'User `{self.follower_user_id}` already is or has requested to follow user `{self.followed_user_id}`'
        )


class FollowerDoesNotExist(FollowerException):
    def __init__(self, follower_user_id, followed_user_id):
        self.follower_user_id = follower_user_id
        self.followed_user_id = followed_user_id
        super().__init__()

    def __str__(self):
        return (
            f'User `{self.follower_user_id}` is not or has not requested to follow user `{self.followed_user_id}`'
        )


class FollowerAlreadyHasStatus(FollowerException):
    def __init__(self, follower_user_id, followed_user_id, follow_status):
        self.follower_user_id = follower_user_id
        self.followed_user_id = followed_user_id
        self.follow_status = follow_status
        super().__init__()

    def __str__(self):
        return (
            f'User `{self.follower_user_id}` already follows user `{self.followed_user_id}` '
            + f'with status `{self.follow_status}`'
        )
