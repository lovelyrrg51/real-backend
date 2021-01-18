class CardException(Exception):
    pass


class CardAlreadyExists(CardException):
    def __init__(self, card_id):
        self.card_id = card_id

    def __str__(self):
        return f'Card `{self.card_id}` already exists'


class MalformedCardId(CardException):
    def __init__(self, card_id):
        self.card_id = card_id

    def __str__(self):
        return f'Card id `{self.card_id}` is malformed'
