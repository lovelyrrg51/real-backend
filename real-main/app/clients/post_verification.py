import requests


class PostVerificationClient:
    def __init__(self, api_creds_getter):
        self.api_creds_getter = api_creds_getter

    @property
    def api_creds(self):
        if not hasattr(self, '_api_creds'):
            self._api_creds = self.api_creds_getter()
        return self._api_creds

    def verify_image(self, image_url, image_format=None, original_format=None, taken_in_real=None):
        headers = {'x-api-key': self.api_creds['key']}
        api_url = self.api_creds['root'] + 'verify/image'

        data = {
            'url': image_url,
            'metadata': {},
        }
        if image_format:
            data['metadata']['imageFormat'] = image_format
        if original_format:
            data['metadata']['originalFormat'] = original_format
        if taken_in_real:
            data['metadata']['takenInReal'] = taken_in_real

        # synchronous for now. Note this generally runs in an async env already: an s3-object-created handler
        resp = requests.post(api_url, headers=headers, json=data)
        if resp.status_code != 200:
            raise Exception(f'Post verification service error `{resp.status_code}` with body `{resp.text}`')
        try:
            return resp.json()['data']['isVerified']
        except Exception as err:
            raise Exception(
                f'Unable to parse response from post verification service with body: `{resp.text}`'
            ) from err
