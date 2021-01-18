class ChatMessageException(Exception):
    pass


class ChatMessageDoesNotExist(ChatMessageException):
    def __init__(self, message_id):
        self.message_id = message_id

    def __str__(self):
        return f'Chat message `{self.message_id}` does not exist'
