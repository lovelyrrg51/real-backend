from uuid import uuid4

import pytest

from app.models.post.dynamo import PostImageDynamo


@pytest.fixture
def post_image_dynamo(dynamo_client):
    yield PostImageDynamo(dynamo_client)


@pytest.fixture
def post_id():
    yield str(uuid4())


@pytest.fixture
def core_item(post_id):
    yield {
        'partitionKey': f'post/{post_id}',
        'schemaVersion': 0,
        'sortKey': 'image',
    }


def test_set_initial_attributes(post_image_dynamo, post_id, core_item):
    assert post_image_dynamo.get(post_id) is None

    # set nothing from nothing, verify
    assert post_image_dynamo.set_initial_attributes(post_id) == {}
    assert post_image_dynamo.get(post_id) is None

    # set just one attribute, verify
    item = post_image_dynamo.set_initial_attributes(post_id, image_format='meh')
    assert post_image_dynamo.get(post_id) == item
    assert item.pop('imageFormat') == 'meh'
    assert item == core_item

    # set an unrelated attributes
    item = post_image_dynamo.set_height_and_width(post_id, height=4, width=10)
    assert post_image_dynamo.get(post_id) == item
    assert item.pop('height') == 4
    assert item.pop('width') == 10
    assert item.pop('imageFormat') == 'meh'
    assert item == core_item

    # verify we can set all attributes, including an overwrite
    crop = {'upperLeft': {'x': 1, 'y': 2}, 'lowerRight': {'x': 3, 'y': 4}}
    item = post_image_dynamo.set_initial_attributes(
        post_id, crop=crop, image_format='if', original_format='of', taken_in_real=True
    )
    assert item.pop('crop') == crop
    assert item.pop('imageFormat') == 'if'
    assert item.pop('originalFormat') == 'of'
    assert item.pop('takenInReal') is True
    assert item.pop('height') == 4
    assert item.pop('width') == 10
    assert item == core_item


def test_media_set_height_and_width(post_image_dynamo, post_id, core_item):
    assert post_image_dynamo.get(post_id) is None

    # set from nothing, verify
    item = post_image_dynamo.set_height_and_width(post_id, 4, 2)
    assert post_image_dynamo.get(post_id) == item
    assert item.pop('height') == 4
    assert item.pop('width') == 2
    assert item == core_item

    # add an unrelated attribute to that item
    item = post_image_dynamo.set_initial_attributes(post_id, image_format='go')
    assert post_image_dynamo.get(post_id) == item
    assert item.pop('imageFormat') == 'go'
    assert item.pop('height') == 4
    assert item.pop('width') == 2
    assert item == core_item

    # set as overwrite, verify
    item = post_image_dynamo.set_height_and_width(post_id, 120, 2000)
    assert post_image_dynamo.get(post_id) == item
    assert item.pop('imageFormat') == 'go'
    assert item.pop('height') == 120
    assert item.pop('width') == 2000
    assert item == core_item


def test_set_colors(post_image_dynamo, post_id, core_item):
    assert post_image_dynamo.get(post_id) is None

    # sample output from ColorTheif
    color_input = [
        (52, 58, 46),
        (186, 206, 228),
        (144, 154, 170),
        (158, 180, 205),
        (131, 125, 125),
    ]
    color_stored = [
        {'r': 52, 'g': 58, 'b': 46},
        {'r': 186, 'g': 206, 'b': 228},
        {'r': 144, 'g': 154, 'b': 170},
        {'r': 158, 'g': 180, 'b': 205},
        {'r': 131, 'g': 125, 'b': 125},
    ]

    # test set from nothing
    item = post_image_dynamo.set_colors(post_id, color_input)
    assert post_image_dynamo.get(post_id) == item
    assert item.pop('colors') == color_stored
    assert item == core_item

    # add some unrelated attributes
    item = post_image_dynamo.set_initial_attributes(post_id, image_format='go')
    assert post_image_dynamo.get(post_id) == item
    assert item.pop('imageFormat') == 'go'
    assert item.pop('colors') == color_stored
    assert item == core_item

    # set as overwrite, verify
    item = post_image_dynamo.set_colors(post_id, [(131, 125, 125)])
    assert post_image_dynamo.get(post_id) == item
    assert item.pop('imageFormat') == 'go'
    assert item.pop('colors') == [{'r': 131, 'g': 125, 'b': 125}]
    assert item == core_item


def test_delete(post_image_dynamo):
    post_id = str(uuid4())
    assert post_image_dynamo.get(post_id) is None

    # deleting an item that doesn't exist fails softly
    post_image_dynamo.delete(post_id)
    assert post_image_dynamo.get(post_id) is None

    # add the post image, verify
    post_image_dynamo.set_initial_attributes(post_id, taken_in_real=True)
    assert post_image_dynamo.get(post_id)

    # delete it, verify
    post_image_dynamo.delete(post_id)
    assert post_image_dynamo.get(post_id) is None
