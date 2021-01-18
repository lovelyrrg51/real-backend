import json

import jwt
import requests

# https://developer.apple.com/documentation/sign_in_with_apple/
# https://pyjwt.readthedocs.io/en/latest/usage.html
# https://gist.github.com/davidhariri/b053787aabc9a8a9cc0893244e1549fe


class AppleClient:
    def __init__(self, public_key_url='https://appleid.apple.com/auth/keys', audience='app.real.mobile'):
        self.public_key_url = public_key_url
        self.audience = audience

    def get_public_key(self, kid, alg):
        # would be good to cache this info but have to be careful not to cache it too long
        payload = requests.get(self.public_key_url).json()
        for key in payload['keys']:
            if key['kid'] == kid and key['alg'] == alg:
                return jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key))
        raise ValueError(f'No Apple public key with kid `{kid}` and alg `{alg}` found')

    def get_verified_email(self, id_token):
        header = jwt.get_unverified_header(id_token)
        public_key = self.get_public_key(header['kid'], header['alg'])
        # To avoid expired signature when testing: jwt.decode(... options={'verify_exp': False})
        info = jwt.decode(id_token, public_key, audience=self.audience, algorithm=header['alg'])
        if not info.get('email_verified') or not info.get('email'):
            raise ValueError('Apple id token does not contain verified email')
        return info['email']
