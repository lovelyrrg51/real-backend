class AppStoreException(Exception):
    pass


class AppStoreSubAlreadyExists(AppStoreException):
    def __init__(self, original_transaction_id):
        self.original_transaction_id = original_transaction_id

    def __str__(self):
        return f'AppStore sub with original transaction ID of `{self.original_transaction_id}` already exists'
