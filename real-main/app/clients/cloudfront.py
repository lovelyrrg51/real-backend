import base64
import json
import os
import urllib

import botocore
import pendulum
from cryptography.hazmat import backends
from cryptography.hazmat.primitives.asymmetric.padding import PKCS1v15
from cryptography.hazmat.primitives.hashes import SHA1
from cryptography.hazmat.primitives.serialization import load_pem_private_key

CLOUDFRONT_UPLOADS_DOMAIN = os.environ.get('CLOUDFRONT_UPLOADS_DOMAIN')


class CloudFrontClient:

    lifetime = pendulum.duration(hours=48)

    def __init__(self, key_pair_getter, domain=CLOUDFRONT_UPLOADS_DOMAIN):
        assert domain, "CloudFront domain is required"
        self.domain = domain
        self.key_pair_getter = key_pair_getter

    def get_key_pair(self):
        if not hasattr(self, '_key_pair'):
            self._key_pair = self.key_pair_getter()
        return self._key_pair

    def get_private_key(self):
        "A PrivateKey object ready to use to .sign()"
        if not hasattr(self, '_private_key'):
            private_key = self.get_key_pair()['privateKey']

            # the private key format requires newlines after the header and before the footer
            # and the secrets manager doesn't seem to play well with newlines
            pk_string = f"-----BEGIN RSA PRIVATE KEY-----\n{private_key}\n-----END RSA PRIVATE KEY-----"
            pk_raw = bytearray(pk_string, 'utf-8')
            backend = backends.default_backend()
            self._private_key = load_pem_private_key(pk_raw, password=None, backend=backend)
        return self._private_key

    def get_cloudfront_signer(self):
        if not hasattr(self, '_cfsigner'):
            key_id = self.get_key_pair()['keyId']
            pk = self.get_private_key()

            def sign(msg):
                return pk.sign(msg, PKCS1v15(), SHA1())

            self._cfsigner = botocore.signers.CloudFrontSigner(key_id, sign)
        return self._cfsigner

    def generate_unsigned_url(self, path):
        return f'https://{self.domain}/{path}'

    def generate_presigned_url(self, path, methods, expires_at=None):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudfront.html#examples
        expires_at = expires_at or pendulum.now('utc') + self.lifetime
        qs = urllib.parse.urlencode([('Method', m) for m in methods])
        url = f'https://{self.domain}/{path}?{qs}'
        return self.get_cloudfront_signer().generate_presigned_url(url, date_less_than=expires_at)

    def generate_presigned_cookies(self, path, expires_at=None):
        # https://gist.github.com/mjohnsullivan/31064b04707923f82484c54981e4749e
        expires_at = expires_at or pendulum.now('utc') + self.lifetime
        url = self.generate_unsigned_url(path)
        policy = self.generate_cookie_policy(url, expires_at)
        signature = self.get_private_key().sign(policy, PKCS1v15(), SHA1())
        return {
            'ExpiresAt': expires_at.to_iso8601_string(),
            'CloudFront-Policy': self._encode(policy),
            'CloudFront-Signature': self._encode(signature),
            'CloudFront-Key-Pair-Id': self.get_key_pair()['keyId'],
        }

    def generate_cookie_policy(self, path, expires_at):
        policy_dict = {
            'Statement': [
                {'Resource': path, 'Condition': {'DateLessThan': {'AWS:EpochTime': expires_at.int_timestamp}}}
            ]
        }
        # Using separators=(',', ':') removes seperator whitespace
        return json.dumps(policy_dict, separators=(',', ':')).encode('utf-8')

    def _encode(self, msg):
        "Base64 encode and replace unsupported chars: '+=/' with '-_~'"
        msg_b64 = str(base64.b64encode(msg), 'utf-8')
        return msg_b64.replace('+', '-').replace('=', '_').replace('/', '~')
