# keep in sync with object created handlers defined serverless.yml


class _ImageSize:
    def __init__(self, name, max_dimensions, content_type='image/jpeg', file_ext='jpg'):
        self.name = name
        self.max_dimensions = max_dimensions
        file_ext = file_ext or self.default_file_ext
        self.filename = f'{self.name}.{file_ext}'
        self.content_type = content_type


NATIVE_HEIC = _ImageSize('native', None, content_type='image/heic', file_ext='heic')
NATIVE = _ImageSize('native', None)
K4 = _ImageSize('4K', (3840, 2160))  # TODO: change name to '4k' with lowercase k
P1080 = _ImageSize('1080p', (1920, 1080))
P480 = _ImageSize('480p', (854, 480))
P64 = _ImageSize('64p', (114, 64))

JPEGS = (NATIVE, K4, P1080, P480, P64)
THUMBNAILS = (K4, P1080, P480, P64)  # ordered by decreasing size
