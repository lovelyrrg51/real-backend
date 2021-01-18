class BlockException(Exception):
    pass


class AlreadyBlocked(BlockException):
    def __init__(self, blocker_user_id, blocked_user_id):
        self.blocker_user_id = blocker_user_id
        self.blocked_user_id = blocked_user_id

    def __str__(self):
        return f'User `{self.blocker_user_id}` has already blocked user `{self.blocked_user_id}`'


class NotBlocked(BlockException):
    def __init__(self, blocker_user_id, blocked_user_id):
        self.blocker_user_id = blocker_user_id
        self.blocked_user_id = blocked_user_id

    def __str__(self):
        return f'User `{self.blocker_user_id}` has not blocked user `{self.blocked_user_id}`'
