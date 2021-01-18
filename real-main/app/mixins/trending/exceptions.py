class TrendingException(Exception):
    pass


class TrendingAlreadyExists(TrendingException):
    def __init__(self, item_type, item_id):
        self.item_type = item_type
        self.item_id = item_id

    def __str__(self):
        return f'Trending for `{self.item_type}:{self.item_id}` already exists'


class TrendingDNEOrAttributeMismatch(TrendingException):
    def __init__(self, item_type, item_id):
        self.item_type = item_type
        self.item_id = item_id

    def __str__(self):
        return f'Trending for `{self.item_type}:{self.item_id}` DNE or does not have expected attributes'
