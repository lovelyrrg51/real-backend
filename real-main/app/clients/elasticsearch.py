import json
import logging
import os

import boto3
import requests
import requests_aws4auth

logger = logging.getLogger()

ELASTICSEARCH_DOMAIN = os.environ.get('ELASTICSEARCH_DOMAIN')


class ElasticSearchClient:

    service = 'es'
    headers = {'Content-Type': 'application/json'}

    def __init__(self, domain=ELASTICSEARCH_DOMAIN):
        assert domain, '`domain` is required'
        self.domain = domain

    @property
    def awsauth(self):
        if not hasattr(self, '_awsauth'):
            session = boto3.Session()
            credentials = session.get_credentials().get_frozen_credentials()
            self._awsauth = requests_aws4auth.AWS4Auth(
                credentials.access_key,
                credentials.secret_key,
                session.region_name,
                self.service,
                session_token=credentials.token,
            )
        return self._awsauth

    def query_users(self, query):
        "`query` should be dict-like structure that can be serialized to json"
        url = f'https://{self.domain}/users/_search'
        resp = requests.get(url, auth=self.awsauth, json={'query': query}, headers=self.headers)
        if resp.status_code != 200:
            logging.warning(f'ElasticSearch: Recieved non-200 response of {resp.status_code} when querying users')
        return resp.json()

    def query_posts(self, query):
        "`query` should be dict-like structure that can be serialized to json"
        url = f'https://{self.domain}/posts/_search'
        resp = requests.post(url, auth=self.awsauth, data=json.dumps(query), headers=self.headers)
        if resp.status_code != 200:
            logging.warning(f'ElasticSearch: Recieved non-200 response of {resp.status_code} when querying posts')
        return resp.json()

    def query_keywords(self, query):
        "`query` should be dict-like structure that can be serialized to json"
        url = f'https://{self.domain}/keywords/_search'
        resp = requests.post(url, auth=self.awsauth, data=json.dumps(query), headers=self.headers)
        if resp.status_code != 200:
            logging.warning(
                f'ElasticSearch: Recieved non-200 response of {resp.status_code} when querying keywords'
            )
        return resp.json()

    def build_user_url(self, user_id):
        return f'https://{self.domain}/users/_doc/{user_id}'

    def build_user_doc(self, user_id, username, full_name):
        doc = {'userId': user_id, 'username': username, 'fullName': full_name}
        return {k: v for k, v in doc.items() if v is not None}

    def build_post_url(self, post_id):
        return f'https://{self.domain}/posts/_doc/{post_id}'

    def build_post_doc(self, post_id, keywords):
        doc = {'postId': post_id, 'keywords': ' '.join(keywords)}
        return {k: v for k, v in doc.items() if v is not None}

    def build_keyword_url(self, post_id, keyword):
        return f'https://{self.domain}/keywords/_doc/{post_id}-{keyword}'

    def build_keyword_doc(self, keyword):
        doc = {'keyword': keyword}
        return {k: v for k, v in doc.items() if v is not None}

    def put_user(self, user_id, username, full_name):
        doc = self.build_user_doc(user_id, username, full_name)
        url = self.build_user_url(user_id)
        logging.info(f'ElasticSearch: Putting user to index at `{url}` ' + json.dumps(doc))
        resp = requests.put(url, auth=self.awsauth, json=doc, headers=self.headers)
        if resp.status_code // 100 != 2:
            logging.warning(f'ElasticSearch: Recieved non-2XX response of {resp.status_code} when adding user')

    def delete_user(self, user_id):
        url = self.build_user_url(user_id)
        logging.info(f'ElasticSearch: Deleting user from index at `{url}`')
        resp = requests.delete(url, auth=self.awsauth)
        if resp.status_code != 200:
            logging.warning(f'ElasticSearch: Recieved non-200 response of {resp.status_code} when deleting user')

    def put_post(self, post_id, keywords):
        doc = self.build_post_doc(post_id, keywords)
        url = self.build_post_url(post_id)
        logging.info(f'ElasticSearch: Putting post to index at `{url}` ' + json.dumps(doc))
        resp = requests.put(url, auth=self.awsauth, json=doc, headers=self.headers)
        if resp.status_code // 100 != 2:
            logging.warning(f'ElasticSearch: Recieved non-2XX response of {resp.status_code} when adding post')

    def delete_post(self, post_id):
        url = self.build_post_url(post_id)
        logging.info(f'ElasticSearch: Deleting post from index at `{url}`')
        resp = requests.delete(url, auth=self.awsauth)
        if resp.status_code != 200:
            logging.warning(f'ElasticSearch: Recieved non-200 response of {resp.status_code} when deleting post')

    def put_keyword(self, post_id, keyword):
        doc = self.build_keyword_doc(keyword)
        url = self.build_keyword_url(post_id, keyword)
        logging.info(f'ElasticSearch: Putting keyword to index at `{url}` ' + json.dumps(doc))
        resp = requests.put(url, auth=self.awsauth, json=doc, headers=self.headers)
        if resp.status_code // 100 != 2:
            logging.warning(f'ElasticSearch: Recieved non-2XX response of {resp.status_code} when adding keyword')

    def delete_keyword(self, post_id, keyword):
        url = self.build_keyword_url(post_id, keyword)
        logging.info(f'ElasticSearch: Deleting keyword from index at `{url}`')
        resp = requests.delete(url, auth=self.awsauth)
        if resp.status_code != 200:
            logging.warning(
                f'ElasticSearch: Recieved non-200 response of {resp.status_code} when deleting keyword'
            )
