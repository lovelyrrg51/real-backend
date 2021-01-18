#!/usr/bin/env python
import argparse
import collections
import datetime
import os
import pprint

import boto3
import dotenv
import pendulum

dotenv.load_dotenv()

COGNITO_USER_POOL_ID = os.environ.get('COGNITO_USER_POOL_ID')
assert COGNITO_USER_POOL_ID, 'Environment variable COGNITO_USER_POOL_ID must be defined'

USER_STASTUSES = [
    'UNCONFIRMED',
    'CONFIRMED',
    'ARCHIVED',
    'COMPROMISED',
    'UNKNOWN',
    'RESET_REQUIRED',
    'FORCE_CHANGE_PASSWORD',
]


def parse_args():
    parser = argparse.ArgumentParser(description="Count users in each possible status in cognito")
    parser.add_argument(
        '-d',
        dest='date',
        type=lambda s: str(pendulum.parse(s).date()),
        help='Only count users that were created in this date. Ex: 2020-05-19',
    )
    parser.add_argument(
        '-s',
        dest='status',
        choices=USER_STASTUSES,
        help='Only count users with the given user status',
    )
    args = parser.parse_args()
    return args.date, args.status


def generate_users(client, attributes_to_get=None, user_filter=None, progress_indicator=None):
    "Return a generator that generates all users in cognito matching the filter"
    kwargs = {'UserPoolId': COGNITO_USER_POOL_ID}
    if attributes_to_get:
        kwargs['AttributesToGet'] = attributes_to_get
    if user_filter:
        kwargs['Filter'] = user_filter

    pagination_token = False
    while pagination_token is not None:
        if progress_indicator:
            print(progress_indicator, end='', flush=True)
        if pagination_token:
            kwargs['PaginationToken'] = pagination_token
        resp = client.list_users(**kwargs)
        for item in resp['Users']:
            yield item
        pagination_token = resp.get('PaginationToken')


def parse_user(user):
    email_verified, phone_number_verified = False, False
    for attr in user['Attributes']:
        if attr['Name'] == 'email_verified' and attr['Value'] == 'true':
            email_verified = True
        if attr['Name'] == 'phone_number_verified' and attr['Value'] == 'true':
            phone_number_verified = True
    return {
        'Username': user['Username'],
        'Enabled': user['Enabled'],
        'UserStatus': user['UserStatus'],
        'UserCreateDate': str(user['UserCreateDate'].astimezone(datetime.timezone.utc).date()),
        'email_verified': email_verified,
        'phone_number_verified': phone_number_verified,
    }


class Stats:
    def __init__(self):
        self.total = 0
        self.create_date = collections.defaultdict(int)
        self.enabled = collections.defaultdict(int)
        self.user_status = collections.defaultdict(int)
        self.email_verified = collections.defaultdict(int)
        self.phone_number_verified = collections.defaultdict(int)

    def apply(self, parsed_user):
        self.total += 1
        self.create_date[parsed_user['UserCreateDate']] += 1
        self.enabled[parsed_user['Enabled']] += 1
        self.user_status[parsed_user['UserStatus']] += 1
        self.email_verified[parsed_user['email_verified']] += 1
        self.phone_number_verified[parsed_user['phone_number_verified']] += 1

    def serialize(self):
        return {
            'total': self.total,
            'UserCreateDate': dict(self.create_date),
            'Enabled': dict(self.enabled),
            'UserStatus': dict(self.user_status),
            'email_verified': dict(self.email_verified),
            'phone_number_verified': dict(self.phone_number_verified),
        }


def main():
    created_date, status = parse_args()
    client = boto3.client('cognito-idp')

    gen = generate_users(client, progress_indicator='.')
    gen = map(parse_user, gen)
    if created_date:
        gen = filter(lambda u: u['UserCreateDate'] == created_date, gen)
    if status:
        gen = filter(lambda u: u['UserStatus'] == status, gen)

    stats = Stats()
    for parsed_user in gen:
        stats.apply(parsed_user)

    print()
    pprint.pprint(stats.serialize())


if __name__ == '__main__':
    main()
