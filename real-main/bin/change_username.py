#!/usr/bin/env python

import argparse
import os
import sys

import dotenv

dotenv.load_dotenv()

# https://stackoverflow.com/questions/16981921
SCRIPT_PATH = os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(SCRIPT_PATH)))
from app.clients import CognitoClient, DynamoClient  # noqa E402
from app.models import UserManager  # noqa E402


def parse_args():
    parser = argparse.ArgumentParser(description="Change a User's username")
    parser.add_argument('-c', dest='current_username', required=True, help='current username')
    parser.add_argument('-u', dest='new_username', required=True, help='desired username')
    args = parser.parse_args()
    return args.current_username, args.new_username


def main():
    current_username, new_username = parse_args()
    clients = {'cognito': CognitoClient(), 'dynamo': DynamoClient()}
    user_manager = UserManager(clients)
    user = user_manager.get_user_by_username(current_username)
    if not user:
        raise Exception(f'No user with username `{current_username}` found')

    print(f"Changing user's username from `{current_username}` to `{new_username}`... ", end='')
    user.update_username(new_username)
    print('done.')


if __name__ == '__main__':
    main()
