class CommentException(Exception):
    pass


class CommentDoesNotExist(CommentException):
    def __init__(self, comment_id):
        self.comment_id = comment_id

    def __str__(self):
        return f'Comment `{self.comment_id}` does not exist'


class CommentAlreadyExists(CommentException):
    def __init__(self, comment_id):
        self.comment_id = comment_id

    def __str__(self):
        return f'Comment `{self.comment_id}` already exists'
