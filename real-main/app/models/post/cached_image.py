import imghdr
import io

import PIL.Image
import PIL.ImageOps
import pyheif

from .exceptions import PostException


class CachedImage:
    def __init__(self, post_id, image_size=None, s3_client=None, s3_path=None, source=None, content_type=None):
        assert (s3_client and s3_path) or source, 'Either s3 kwargs or source kwargs required'

        self.post_id = post_id
        self.image_size = image_size
        self.s3_client = s3_client
        self.s3_path = s3_path
        self.source = source
        self.content_type = content_type or (image_size.content_type if image_size else None)

        # if self._image is set, that's the latest data
        # if self._image is not set, then self._data will contain the latest data
        self._data = None
        self._image = None

        # Possible values and meanings:
        #   - True: what's in the cache is known to match the source
        #   - False: what's in the cache is thought to be different than the source
        #   - None: cache has never been filled
        self.is_synced = None

    @property
    def readonly_image(self):
        """
        It's not really readonly, the name is just to scare the client into not mutating it.
        Use readonly_image.copy() first if you want to make changes.
        """
        if not self._image and not self._data:
            self.refresh()
        if not self._image and self._data:
            self._fill_image_from_data()
        return self._image

    def _fill_image_from_data(self):
        fh = io.BytesIO(self._data)
        if self.content_type == 'image/heic':
            try:
                heif_file = pyheif.read(fh)
            except (ValueError, pyheif.error.HeifError) as err:
                raise PostException(f'Unable to read HEIC file for post `{self.post_id}`: {err}') from err
            self._image = PIL.Image.frombytes(
                heif_file.mode, heif_file.size, heif_file.data, 'raw', heif_file.mode, heif_file.stride
            )
        elif self.content_type == 'image/jpeg':
            file_type = imghdr.what(fh)
            if file_type is None:
                raise PostException(f'Unable to recognize file type of uploaded file for post `{self.post_id}`')
            if file_type != 'jpeg' and file_type != 'png':
                raise PostException(f'File of type `{file_type}` for uploaded jpeg image post `{self.post_id}`')
            try:
                self._image = PIL.ImageOps.exif_transpose(PIL.Image.open(fh))
            except Exception as err:
                raise PostException(f'Unable to decode jpeg data for post `{self.post_id}`: {err}') from err
        else:
            raise PostException(f'Unrecognized content-type `{self.content_type}`')

    def set_image(self, image):
        self._data = None
        self._image = image.copy()
        self.is_synced = False
        return self

    def set_data(self, fh):
        fh.seek(0)
        self._data = fh.read()
        self._image = None
        self.is_synced = False
        return self

    def clear(self):
        if not (self.is_synced and self._image is None and self._data is None):
            self._data = None
            self._image = None
            self.is_synced = False
        return self

    def refresh(self):
        if self.source:
            self._data = None
            self._image = self.source()
        else:
            try:
                fh = self.s3_client.get_object_data_stream(self.s3_path)
            except self.s3_client.exceptions.NoSuchKey as err:
                raise PostException(f'{self.s3_path} image data not found for post `{self.post_id}`') from err
            self._data = fh.read()
            self._image = None
        self.is_synced = True
        return self

    def crop(self, crop):
        cur_width, cur_height = self.readonly_image.size
        ul_x, ul_y = crop['upperLeft']['x'], crop['upperLeft']['y']
        lr_x, lr_y = crop['lowerRight']['x'], crop['lowerRight']['y']

        if lr_y > cur_height:
            raise PostException('Image not tall enough to crop as requested')
        if lr_x > cur_width:
            raise PostException('Image not wide enough to crop as requested')

        if ul_x == 0 and ul_y == 0 and lr_x == cur_width and lr_y == cur_height:
            return self

        try:
            self._image = self.readonly_image.crop((ul_x, ul_y, lr_x, lr_y))
        except Exception as err:
            raise PostException(f'Unable to crop image for post `{self.id}`: {err}') from err

        self._data = None
        self.is_synced = False
        return self

    def flush(self, include_deletes=False):
        assert self.s3_path, 'Can only flush cached images backed by S3'
        if self.is_synced is None:
            raise Exception('Nothing to flush back')
        if self.is_synced is False:
            if not self._data and not self._image:
                if not include_deletes:
                    raise Exception('Refusing to flush back empty cache without `include_deletes` kwarg')
                self.s3_client.delete_object(self.s3_path)
            else:
                if self._data:
                    fh = io.BytesIO(self._data)
                elif self._image:
                    assert self.content_type == 'image/jpeg', 'Non-jpeg images can only be flushed back empty'
                    fh = io.BytesIO()
                    # Note: Pillow's Image.save treats None differently than not present for some kwargs
                    kwargs = {
                        k: v
                        for k, v in {
                            'format': 'JPEG',
                            'quality': 100,  # per spec
                            'icc_profile': self._image.info.get('icc_profile'),
                            'exif': self._image.info.get('exif'),
                        }.items()
                        if v is not None
                    }
                    try:
                        self._image.convert('RGB').save(fh, **kwargs)
                    except Exception as err:
                        raise PostException(f'Unable to save pil image for post `{self.post_id}`: {err}') from err
                    fh.seek(0)
                self.s3_client.put_object(self.s3_path, fh, self.content_type)
            self.is_synced = True
        return self
