import string

from real_auth.validate import validate_username


def test_validate_username_length():
    assert validate_username('') is False
    assert validate_username(None) is False

    # too short
    assert validate_username('a') is False
    assert validate_username('24') is False

    # too long
    assert validate_username('4' * 31) is False

    # just right
    assert validate_username('aaa') is True
    assert validate_username('4' * 30) is True


def test_validate_username_bad_chars():
    bad_chars = set(string.printable) - set(string.digits + string.ascii_letters + '_.')
    for bad_char in bad_chars:
        assert validate_username('aaa' + bad_char) is False
        assert validate_username(bad_char + 'aaa') is False
        assert validate_username(bad_char * 3) is False
