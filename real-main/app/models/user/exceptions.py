class UserException(Exception):
    pass


class UserValidationException(UserException):
    pass


class UserVerificationException(UserException):
    pass


class UserDoesNotExist(UserException):
    def __init__(self, user_id):
        self.user_id = user_id

    def __str__(self):
        return f'User `{self.user_id}` does not exist'


class UserAlreadyExists(UserException):
    def __init__(self, user_id):
        self.user_id = user_id

    def __str__(self):
        return f'User `{self.user_id}` already exists'


class UserAlreadyGrantedSubscription(UserException):
    def __init__(self, user_id):
        self.user_id = user_id

    def __str__(self):
        return f'User `{self.user_id}` has already granted themselves a subscription bonus'
