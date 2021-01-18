import urllib

from app.clients import CloudFrontClient

# format of the entry that is stored in the AWS secrets manager
# this is a formerly valid key pair
testing_only_key_pair = {
    'keyId': 'APKAJ5VEBTYPMFSVR5RA',
    'publicKey': """MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAh932Hnm7p4jS9EBfAhVi
f7J24WIGNUPw75AAu3Fm9qFxiQtVrUZTw6VpO1Qa2IiLRfXzbwAzVd8miXsbt6Qy
WU5jZ8Ripr9O31z1OQMolL0qYSoDShrNlppo5h4i9jWwo/5dReTk/FJf2Pavw8Cw
a9nTEF5VVwcXsFHjReRa8lwLxj+T7p6eVM+GZO66yt8VwZhom//U+U11uGu2TKBe
oVq6uQu5QMjl2M2EA43G5/85yLik1oxR+b8eMmzOOYgpzJ2ufGA6+1sD3V+LyxpX
BqNwQzoQ3CDYhnHVsB/sfo43ny7Q7aK3V+v10HW69VXsMCAJw1Nk8kf1bnE88kNb
NQIDAQAB""",
    'privateKey': """MIIEpAIBAAKCAQEAh932Hnm7p4jS9EBfAhVif7J24WIGNUPw75AAu3Fm9qFxiQtV
rUZTw6VpO1Qa2IiLRfXzbwAzVd8miXsbt6QyWU5jZ8Ripr9O31z1OQMolL0qYSoD
ShrNlppo5h4i9jWwo/5dReTk/FJf2Pavw8Cwa9nTEF5VVwcXsFHjReRa8lwLxj+T
7p6eVM+GZO66yt8VwZhom//U+U11uGu2TKBeoVq6uQu5QMjl2M2EA43G5/85yLik
1oxR+b8eMmzOOYgpzJ2ufGA6+1sD3V+LyxpXBqNwQzoQ3CDYhnHVsB/sfo43ny7Q
7aK3V+v10HW69VXsMCAJw1Nk8kf1bnE88kNbNQIDAQABAoIBADYomE8Vn2Ps+oo6
jqS5+YWFkjXNaUQaTRRxhpkxXyW4vRUv23syqXk3mnb306u04i+FFwCMR+pXBXmQ
BUByx08qHB3k/p4RbFNFLssHI0oHDbmlkaIchQ8fhekU4kLarAry/iM6Vrzt6R8H
VpDlYm3banL/52zFev/h5IKbwFUTGdGDl09pgGPFrELOwrH+RRfHaBFSZrZmVIWs
IUII/NY+4LXA+7KSrvIn2kfeFhB5gBJY72sK2X/q9pITO4Ru82r0Nw5DbALSttbH
2OKv1XEH+hwn7f/+DoP3w2ujiqTbdQLovVGu408c/SXHFVoUEtxUtr0GnsgYvJcN
aYJT+AECgYEA9CkIKUjfnmvGT+cFMD5HIaH4fIt6ACDpTATV8ZQkCRdZ+RQpgdqF
ntFOQf8GybQu8x43MJa96EhsziL8QeVFoDMpMDdDvkYytdVjdcH04FnWeawPeoC8
pD10GoEZ6Ye/tIR9xK1kTJUFajkmUVOi88XbrPNrW/rmvYS/lQJl1IECgYEAjnSY
B+ELUcghAWnaR1FbPtBwxNIOg2yCUQRJYfXi7O2RXetEHjMgLj+FuKHxCf60esLg
c7NgxYt9wjGaMx5Ww9QZLqKc1lSqTqTJzHnfLk7AY6MaBlhzd31CLSMeGITWpZ09
7AvBkA7S04fnO9Zd5vfaa5yfXUsIRaxegyIGHLUCgYEAjorp9dhSnRWEmJ9h+xFQ
y8TY4jU3i52rNjNYiAoZo6kbYPwxY1slSVwe2Q6/csCb7FnGlLpcsqCdzRbFuN7W
cDmOIVUSWqJ29otW3qfWg6hPO4eFHdrMxwINp2+ZpioXdJcpKcqk1MTnfWVSBobS
iokHwAf4tKFdVmWKx763noECgYB08x1Y4pUzZ7RI/8jWYeEh+WeK/dQyauO1dWp1
RLMuxX5g92Nt05UowreaM0C6buNmIRS5h9r7cqAkzCoGq3KZxEeENLXDc7B5benJ
t4fU8YwagG0+JmFtCGVKvxjXEj9RqXyLi7818CV+yYS5aCyhEHu9etCOe6nn4TGa
QFV6PQKBgQDhtLKiDEHEvqFlBtq1Q96ygUxuAeDlZGlxbQ2HAHWUekrioVUEMvfW
AZE26Kr3XavICBdklQAwRCy7jq+rz/UPTEQVP2+YFdRCCKDzKblF7eZ7MfZ/GtDn
HffduWEgioz0H0hcvYygjOwat/1zv6ZmRq8rZ2rwRr6KhOvlNfR/Fw==""",
}


def get_key_pair():
    return testing_only_key_pair


def test_generate_presigned_url():
    domain = 'random-domain-stirng.cloudfront.net'
    client = CloudFrontClient(get_key_pair, domain=domain)
    path = 'uid/mid'
    methods = ['M1', 'M2']

    signed_url = client.generate_presigned_url(path, methods)

    # would be ideal to test if this is really signed correctly...
    # but this will catch 99% of the bugs with 1% of the work

    parsed = urllib.parse.urlparse(signed_url)
    assert parsed.scheme == 'https'
    assert parsed.netloc == domain
    assert parsed.path == f'/{path}'

    parsed_qs = urllib.parse.parse_qs(parsed.query)
    assert set(parsed_qs.keys()) == set(['Method', 'Expires', 'Key-Pair-Id', 'Signature'])
    assert set(parsed_qs['Method']) == set(methods)
