class FlagException(Exception):
    pass


class AlreadyFlagged(FlagException):
    def __init__(self, item_type, item_id, user_id):
        super().__init__()
        # capitalize() lower-cases the rest of the string, which we don't want
        self.item_type_humanized = item_type[0].capitalize() + item_type[1:]
        self.item_id = item_id
        self.user_id = user_id

    def __str__(self):
        return f'{self.item_type_humanized} `{self.item_id}` has already been flagged by user `{self.user_id}`'


class NotFlagged(FlagException):
    def __init__(self, item_type, item_id, user_id):
        super().__init__()
        # capitalize() lower-cases the rest of the string, which we don't want
        self.item_type_humanized = item_type[0].capitalize() + item_type[1:]
        self.item_id = item_id
        self.user_id = user_id

    def __str__(self):
        return f'{self.item_type_humanized} `{self.item_id}` has not been flagged by user `{self.user_id}`'
