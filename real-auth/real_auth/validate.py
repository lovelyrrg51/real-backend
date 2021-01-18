"""
For now, this logic is duplicated between the real-main and real-auth stacks.
Keep them in sync.
"""

import re

username_regex = re.compile('[a-zA-Z0-9_.]{3,30}')


def validate_username(username):
    if not username:
        return False

    matched_username = username_regex.match(username)  # matches only from beginging of string
    if not matched_username or matched_username[0] != username:
        return False

    return True
