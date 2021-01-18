"""
Tests for generating images from text.

These tests aren't intended to ensure the output looks correct,
they're more just intended to ensure the alogirthm doesn't crash.
"""
import pytest

from app.models.post.text_image import generate_text_image, rectangle_wrap

dims_4k = (3840, 2160)
dims_64p = (114, 64)


def test_genearate_text_image():
    # test no text
    with pytest.raises(AssertionError):
        generate_text_image('', dims_4k).read(1)

    # test one long word on a small image
    assert generate_text_image('supercalifragilisticexpialidocious', dims_64p)

    # test two words
    assert generate_text_image('Fly high', dims_4k)

    # test a general message
    assert generate_text_image('Today for lunch I had a burger. It was really good', dims_4k)

    # test a long message
    msg = ('And you, what did you have for lunch today? ' * 10).strip()
    assert generate_text_image(msg, dims_4k)


def test_rectangle_wrap():
    token_spacing = 2
    line_spacing = 2
    line_height = 10
    desired_aspect_ratio = 16 / 9

    raw_tokens = ['a', 'b', 'c', 'd', 'e']
    token_widths = [15, 13, 16, 14, 17]

    text, text_width, text_height = rectangle_wrap(
        raw_tokens, token_widths, token_spacing, line_spacing, line_height, desired_aspect_ratio
    )
    assert text == 'a b c\nd e'
    assert text_height == 22
    assert text_width == 48
