import cachecontrol
import requests
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token


class GoogleClient:
    def __init__(self, client_ids_getter):
        self.client_ids_getter = client_ids_getter
        self.cached_session = cachecontrol.CacheControl(requests.session())

    @property
    def client_ids(self):
        if not hasattr(self, '_client_ids'):
            self._client_ids = self.client_ids_getter()
        return self._client_ids

    def get_verified_email(self, id_token):
        "Verify the token, parse and return a verified email from it"
        # https://developers.google.com/oauthplayground/
        # https://developers.google.com/identity/sign-in/web/backend-auth#calling-the-tokeninfo-endpoint
        # https://googleapis.dev/python/google-auth/latest/reference/google.oauth2.id_token.html
        # raises ValueError on expired token
        info = google_id_token.verify_oauth2_token(id_token, google_requests.Request(session=self.cached_session))
        if info.get('aud') not in self.client_ids.values():
            raise ValueError(f'Google token wrong audience: `{info["aud"]}`')
        if not info.get('email_verified') or not info.get('email'):
            raise ValueError('Google token does not contain verified email')
        return info['email']
