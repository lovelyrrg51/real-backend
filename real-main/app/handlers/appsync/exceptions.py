class ClientException(Exception):
    "Any error attributable to the graphql client"

    def __init__(self, msg, info=None):
        self.msg = msg
        self.info = info
        super().__init__()

    def __str__(self):
        return f'ClientError: {self.msg}'

    def serialize(self):
        return {
            'type': 'ClientError',
            'message': str(self),
            'info': self.info,
        }
