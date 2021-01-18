import logging
from uuid import uuid4

import pendulum
import pytest

from app.models.comment.dynamo import CommentDynamo
from app.models.comment.exceptions import CommentAlreadyExists


@pytest.fixture
def comment_dynamo(dynamo_client):
    yield CommentDynamo(dynamo_client)


def test_add_comment(comment_dynamo):
    comment_id = 'cid'
    post_id = 'pid'
    user_id = 'uid'
    text = 'text @dog'
    text_tags = [{'tag': '@dog', 'userId': 'duid'}]
    now = pendulum.now('utc')

    # add the comment to the DB, verify format
    comment_item = comment_dynamo.add_comment(comment_id, post_id, user_id, text, text_tags, now)
    assert comment_dynamo.get_comment(comment_id) == comment_item
    assert comment_item == {
        'partitionKey': 'comment/cid',
        'sortKey': '-',
        'schemaVersion': 1,
        'gsiA1PartitionKey': 'comment/pid',
        'gsiA1SortKey': now.to_iso8601_string(),
        'gsiA2PartitionKey': 'comment/uid',
        'gsiA2SortKey': now.to_iso8601_string(),
        'commentId': 'cid',
        'postId': 'pid',
        'userId': 'uid',
        'text': text,
        'textTags': text_tags,
        'commentedAt': now.to_iso8601_string(),
    }


def test_cant_add_comment_same_comment_id(comment_dynamo):
    comment_id = 'cid'
    post_id = 'pid'
    user_id = 'uid'
    text = 'lore'
    text_tags = []

    # add a comment with that comment id
    comment_dynamo.add_comment(comment_id, post_id, user_id, text, text_tags)

    # verify we can't add another comment with the same id
    with pytest.raises(CommentAlreadyExists):
        comment_dynamo.add_comment(comment_id, post_id, user_id, text, text_tags)


def test_delete_comment(comment_dynamo):
    comment_id = 'cid'
    post_id = 'pid'
    user_id = 'uid'
    text = 'lore'
    text_tags = []

    # delete a comment that doesn't exist
    assert comment_dynamo.delete_comment(comment_id) is None

    # add the comment, verify
    comment_dynamo.add_comment(comment_id, post_id, user_id, text, text_tags)
    comment_item = comment_dynamo.get_comment(comment_id)
    assert comment_item['commentId'] == comment_id

    # delete the comment, verify
    assert comment_dynamo.delete_comment(comment_id)
    assert comment_dynamo.get_comment(comment_id) is None


def test_generate_by_post(comment_dynamo):
    post_id = 'pid'

    # add a comment on an unrelated post
    comment_dynamo.add_comment('coid', 'poid', 'uiod', 't', [])

    # post has no comments, generate them
    assert list(comment_dynamo.generate_by_post(post_id)) == []

    # add two comments to that post
    comment_id_1 = 'cid1'
    comment_id_2 = 'cid2'
    comment_dynamo.add_comment(comment_id_1, post_id, 'uid1', 't', [])
    comment_dynamo.add_comment(comment_id_2, post_id, 'uid1', 't', [])

    # generate comments, verify order
    comment_items = list(comment_dynamo.generate_by_post(post_id))
    assert len(comment_items) == 2
    assert comment_items[0]['commentId'] == comment_id_1
    assert comment_items[1]['commentId'] == comment_id_2


def test_generate_by_user(comment_dynamo):
    user_id = 'uid'

    # add a comment by an unrelated user
    comment_dynamo.add_comment('coid', 'poid', 'uiod', 't', [])

    # user has no comments, generate them
    assert list(comment_dynamo.generate_by_user(user_id)) == []

    # add two comments by that user
    comment_id_1 = 'cid1'
    comment_id_2 = 'cid2'
    comment_dynamo.add_comment(comment_id_1, 'pid1', user_id, 't', [])
    comment_dynamo.add_comment(comment_id_2, 'pid2', user_id, 't', [])

    # generate comments, verify order
    comment_items = list(comment_dynamo.generate_by_user(user_id))
    assert len(comment_items) == 2
    assert comment_items[0]['commentId'] == comment_id_1
    assert comment_items[1]['commentId'] == comment_id_2


def test_generate_all_comments_by_scan(comment_dynamo):
    user_id_1 = 'uid1'
    user_id_2 = 'uid2'

    comment_id_1 = 'cid1'
    comment_id_2 = 'cid2'
    comment_id_3 = 'cid3'

    comment_dynamo.add_comment(comment_id_1, 'pid1', user_id_1, 't', [])
    comment_dynamo.add_comment(comment_id_2, 'pid2', user_id_1, 't', [])
    comment_dynamo.add_comment(comment_id_3, 'pid3', user_id_2, 't', [])

    pks = [pk['partitionKey'].split('/')[1] for pk in comment_dynamo.generate_all_comments_by_scan()]
    assert pks == [comment_id_1, comment_id_2, comment_id_3]


@pytest.mark.parametrize(
    'incrementor_name, decrementor_name, attribute_name',
    [['increment_flag_count', 'decrement_flag_count', 'flagCount']],
)
def test_increment_decrement_count(comment_dynamo, caplog, incrementor_name, decrementor_name, attribute_name):
    incrementor = getattr(comment_dynamo, incrementor_name)
    decrementor = getattr(comment_dynamo, decrementor_name) if decrementor_name else None
    comment_id = str(uuid4())

    # can't increment comment that doesnt exist
    with caplog.at_level(logging.WARNING):
        assert incrementor(comment_id) is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'WARNING'
    assert all(x in caplog.records[0].msg for x in ['Failed to increment', attribute_name, comment_id])
    caplog.clear()

    # can't decrement comment that doesnt exist
    if decrementor:
        with caplog.at_level(logging.WARNING):
            assert decrementor(comment_id) is None
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == 'WARNING'
        assert all(x in caplog.records[0].msg for x in ['Failed to decrement', attribute_name, comment_id])
        caplog.clear()

    # add the comment to the DB, verify it is in DB
    comment_dynamo.add_comment(comment_id, str(uuid4()), str(uuid4()), 'lore', [])
    assert attribute_name not in comment_dynamo.get_comment(comment_id)

    assert incrementor(comment_id)[attribute_name] == 1
    assert comment_dynamo.get_comment(comment_id)[attribute_name] == 1
    assert incrementor(comment_id)[attribute_name] == 2
    assert comment_dynamo.get_comment(comment_id)[attribute_name] == 2

    if decrementor:
        # decrement twice, verify
        assert decrementor(comment_id)[attribute_name] == 1
        assert comment_dynamo.get_comment(comment_id)[attribute_name] == 1
        assert decrementor(comment_id)[attribute_name] == 0
        assert comment_dynamo.get_comment(comment_id)[attribute_name] == 0

        # verify fail soft on trying to decrement below zero
        with caplog.at_level(logging.WARNING):
            resp = decrementor(comment_id)
        assert resp is None
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == 'WARNING'
        assert all(x in caplog.records[0].msg for x in ['Failed to decrement', attribute_name, comment_id])
        assert comment_dynamo.get_comment(comment_id)[attribute_name] == 0
