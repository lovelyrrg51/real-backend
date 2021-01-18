"""
Tests for generating album art.

These tests mainly intended to just ensure the art-generating logic
doesn't crash, not that the output has the correct visual form.
"""
from os import path

import PIL.Image
import pytest

from app.models.album import art

grant_path = path.join(path.dirname(__file__), '..', '..', 'fixtures', 'grant.jpg')
grant_horz_path = path.join(path.dirname(__file__), '..', '..', 'fixtures', 'grant-horizontal.jpg')
grant_vert_path = path.join(path.dirname(__file__), '..', '..', 'fixtures', 'grant-vertical.jpg')
big_blank_path = path.join(path.dirname(__file__), '..', '..', 'fixtures', 'big-blank.jpg')

grant_size = (240, 320)
grant_horz_size = (240, 120)
grant_vert_size = (120, 320)
big_blank_size = (4000, 2000)


def get_images(cnt):
    paths = [grant_path, grant_horz_path, grant_vert_path, grant_path, big_blank_path]
    return [PIL.Image.open(paths[i % 5]) for i in range(cnt)]


@pytest.mark.parametrize('cnt', [0, 1, 2, 3, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20])
def test_generate_basic_grid_failures(cnt):
    with pytest.raises(AssertionError):
        art.generate_basic_grid(get_images(cnt))


@pytest.mark.parametrize('cnt, size', [[4, (480, 640)], [9, (12000, 6000)], [16, (16000, 8000)]])
def test_generate_baisc_grid_success(cnt, size):
    assert (image := art.generate_basic_grid(get_images(cnt)))
    assert image.size == size


@pytest.mark.parametrize('cnt', [0, 1, 2, 3, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20])
def test_generate_zoomed_grid_failures(cnt):
    with pytest.raises(AssertionError):
        art.generate_zoomed_grid(get_images(cnt))


@pytest.mark.parametrize('cnt, size', [[4, (3840, 2160)], [9, (3840, 2160)], [16, (3840, 2160)]])
def test_generate_zoomed_grid_success(cnt, size):
    assert (image := art.generate_zoomed_grid(get_images(cnt)))
    assert image.size == size
