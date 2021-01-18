class ViewException(Exception):
    pass


class ViewAlreadyExists(ViewException):
    def __init__(self, item_type, item_id, user_id):
        self.item_type = item_type
        self.item_id = item_id
        self.user_id = user_id

    def __str__(self):
        return f'View for `{self.item_type}:{self.item_id}` by user `{self.user_id}` already exists'


class ViewDoesNotExist(ViewException):
    def __init__(self, item_type, item_id, user_id):
        self.item_type = item_type
        self.item_id = item_id
        self.user_id = user_id

    def __str__(self):
        return f'View for `{self.item_type}: {self.item_id}` by user `{self.user_id}` does not exist'
