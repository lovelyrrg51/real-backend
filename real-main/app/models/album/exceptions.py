class AlbumException(Exception):
    pass


class AlbumDoesNotExist(AlbumException):
    def __init__(self, album_id):
        self.album_id = album_id

    def __str__(self):
        return f'Album `{self.album_id}` does not exist'


class AlbumAlreadyExists(AlbumException):
    def __init__(self, album_id):
        self.album_id = album_id

    def __str__(self):
        return f'Album `{self.album_id}` already exists'
