class ChatException(Exception):
    pass


class ChatDoesNotExist(ChatException):
    def __init__(self, chat_id):
        self.chat_id = chat_id

    def __str__(self):
        return f'Chat `{self.chat_id}` does not exist'
