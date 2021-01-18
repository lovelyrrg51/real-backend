from uuid import uuid4

import pendulum
import pytest

from app.models.card.dynamo import CardDynamo
from app.models.card.exceptions import CardAlreadyExists


@pytest.fixture
def card_dynamo(dynamo_client):
    yield CardDynamo(dynamo_client)


def test_add_card_minimal(card_dynamo):
    card_id = str(uuid4())
    user_id = str(uuid4())
    title = 'you should know this'
    action = 'https://some-valid-url.com'

    # add the card to the DB
    before = pendulum.now('utc')
    card_item = card_dynamo.add_card(card_id, user_id, title, action)
    after = pendulum.now('utc')

    # retrieve the card and verify the format is as we expect
    assert card_dynamo.get_card(card_id) == card_item
    created_at_str = card_item['gsiA1SortKey'][len('card/') :]
    assert before < pendulum.parse(created_at_str) < after
    assert card_item == {
        'partitionKey': f'card/{card_id}',
        'sortKey': '-',
        'schemaVersion': 1,
        'gsiA1PartitionKey': f'user/{user_id}',
        'gsiA1SortKey': f'card/{created_at_str}',
        'title': title,
        'action': action,
    }

    # verify we can't add that card again
    with pytest.raises(CardAlreadyExists):
        card_dynamo.add_card(card_id, user_id, title, action)


def test_add_card_maximal(card_dynamo):
    card_id = str(uuid4())
    user_id = str(uuid4())
    title = 'you should know this'
    action = 'https://some-valid-url.com'
    sub_title = 'more info for you'
    created_at = pendulum.now('utc')
    notify_user_at = pendulum.now('utc')
    post_id = str(uuid4())
    comment_id = str(uuid4())

    # add the card to the DB
    card_item = card_dynamo.add_card(
        card_id,
        user_id,
        title,
        action,
        sub_title=sub_title,
        created_at=created_at,
        notify_user_at=notify_user_at,
        post_id=post_id,
        comment_id=comment_id,
    )

    # retrieve the card and verify the format is as we expect
    assert card_dynamo.get_card(card_id) == card_item
    assert card_item == {
        'partitionKey': f'card/{card_id}',
        'sortKey': '-',
        'schemaVersion': 1,
        'gsiA1PartitionKey': f'user/{user_id}',
        'gsiA1SortKey': f'card/{created_at.to_iso8601_string()}',
        'title': title,
        'action': action,
        'subTitle': sub_title,
        'postId': post_id,
        'commentId': comment_id,
        'gsiA2PartitionKey': f'card/{post_id}',
        'gsiA2SortKey': user_id,
        'gsiA3PartitionKey': f'card/{comment_id}',
        'gsiA3SortKey': '-',
        'gsiK1PartitionKey': 'card',
        'gsiK1SortKey': notify_user_at.to_iso8601_string() + '/' + user_id,
    }


def test_update_title(card_dynamo):
    # add a card, check title
    card_id = str(uuid4())
    org_card_item = card_dynamo.add_card(card_id, 'uid', 'first-title', 'a')
    assert card_dynamo.get_card(card_id) == org_card_item
    assert org_card_item['title'] == 'first-title'

    # update title, verify
    card_item = card_dynamo.update_title(card_id, 'new title')
    assert card_dynamo.get_card(card_id) == card_item
    assert card_item['title'] == 'new title'
    org_card_item['title'] = card_item['title']
    assert org_card_item == card_item


def test_clear_notify_user_at(card_dynamo):
    # add a card with a notify_user_at, verify
    card_id = str(uuid4())
    org_card_item = card_dynamo.add_card(card_id, 'uid', 't', 'a', notify_user_at=pendulum.now('utc'))
    assert 'gsiK1PartitionKey' in org_card_item
    assert 'gsiK1SortKey' in org_card_item

    # clear notify user at, verify
    card_item = card_dynamo.clear_notify_user_at(card_id)
    assert 'gsiK1PartitionKey' not in card_item
    assert 'gsiK1SortKey' not in card_item
    assert org_card_item.pop('gsiK1PartitionKey')
    assert org_card_item.pop('gsiK1SortKey')
    assert card_item == org_card_item
    assert card_dynamo.get_card(card_id) == card_item

    # clear notify user at, verify idempotent
    assert card_dynamo.clear_notify_user_at(card_id) == card_item
    assert card_dynamo.get_card(card_id) == card_item


def test_delete_card(card_dynamo):
    # delelte a card that DNE
    card_id = str(uuid4())
    assert card_dynamo.delete_card(card_id) is None

    # add the card, verify
    card_dynamo.add_card(card_id, 'uid', 'title', 'https://go.go')
    assert card_dynamo.get_card(card_id)

    # delete the card, verify
    card_dynamo.delete_card(card_id)
    assert card_dynamo.get_card(card_id) is None


def test_generate_cards_by_user(card_dynamo):
    user_id = 'uid'

    # add a card by an unrelated user
    card_dynamo.add_card('coid', 'uoid', 'title', 'https://a.b')

    # user has no cards, generate them
    assert list(card_dynamo.generate_cards_by_user(user_id)) == []

    # add one card
    card_dynamo.add_card('cid1', user_id, 'title1', 'https://a.b')

    # generate the one card
    card_items = list(card_dynamo.generate_cards_by_user(user_id))
    assert len(card_items) == 1
    assert card_items[0]['partitionKey'] == 'card/cid1'
    assert card_items[0]['title'] == 'title1'

    # add another card
    card_dynamo.add_card('cid2', user_id, 'title2', 'https://c.d')

    # generate two cards, check order
    card_items = list(card_dynamo.generate_cards_by_user(user_id))
    assert len(card_items) == 2
    assert card_items[0]['partitionKey'] == 'card/cid1'
    assert card_items[0]['title'] == 'title1'
    assert card_items[1]['partitionKey'] == 'card/cid2'
    assert card_items[1]['title'] == 'title2'

    # generate two cards, pks_only
    card_items = list(card_dynamo.generate_cards_by_user(user_id, pks_only=True))
    assert len(card_items) == 2
    assert card_items[0] == {'partitionKey': 'card/cid1', 'sortKey': '-'}
    assert card_items[1] == {'partitionKey': 'card/cid2', 'sortKey': '-'}


def test_generate_cards_by_post(card_dynamo):
    post_id_1 = str(uuid4())
    post_id_2 = str(uuid4())
    user_id_1 = str(uuid4())
    user_id_2 = str(uuid4())

    # add cards for both users for post 1, and only one user for post 2
    card_id_11 = str(uuid4())
    card_id_12 = str(uuid4())
    card_id_21 = str(uuid4())
    card_dynamo.add_card(card_id_11, user_id_1, 't', 'a', post_id=post_id_1)
    card_dynamo.add_card(card_id_12, user_id_1, 't', 'a', post_id=post_id_2)
    card_dynamo.add_card(card_id_21, user_id_2, 't', 'a', post_id=post_id_1)

    # verify generating all the possiblities with user specified
    assert list(card_dynamo.generate_card_keys_by_post(post_id_1, user_id=user_id_1)) == [
        {'partitionKey': f'card/{card_id_11}', 'sortKey': '-'}
    ]
    assert list(card_dynamo.generate_card_keys_by_post(post_id_2, user_id=user_id_1)) == [
        {'partitionKey': f'card/{card_id_12}', 'sortKey': '-'}
    ]
    assert list(card_dynamo.generate_card_keys_by_post(post_id_1, user_id=user_id_2)) == [
        {'partitionKey': f'card/{card_id_21}', 'sortKey': '-'}
    ]
    assert list(card_dynamo.generate_card_keys_by_post(post_id_2, user_id=user_id_2)) == []

    # verify generating all the possiblities without user specified
    keys = list(card_dynamo.generate_card_keys_by_post(post_id_1))
    assert len(keys) == 2
    assert {'partitionKey': f'card/{card_id_21}', 'sortKey': '-'} in keys
    assert {'partitionKey': f'card/{card_id_11}', 'sortKey': '-'} in keys
    assert list(card_dynamo.generate_card_keys_by_post(post_id_2)) == [
        {'partitionKey': f'card/{card_id_12}', 'sortKey': '-'},
    ]


def test_generate_cards_by_comment(card_dynamo):
    comment_id_1 = str(uuid4())
    comment_id_2 = str(uuid4())
    user_id_1 = str(uuid4())
    user_id_2 = str(uuid4())

    # add cards for both users for post 1, and only one user for post 2
    card_id_11 = str(uuid4())
    card_id_12 = str(uuid4())
    card_id_21 = str(uuid4())
    card_dynamo.add_card(card_id_11, user_id_1, 't', 'a', comment_id=comment_id_1)
    card_dynamo.add_card(card_id_12, user_id_1, 't', 'a', comment_id=comment_id_2)
    card_dynamo.add_card(card_id_21, user_id_2, 't', 'a', comment_id=comment_id_1)

    # verify generating all the possiblities
    keys = list(card_dynamo.generate_card_keys_by_comment(comment_id_1))
    assert len(keys) == 2
    assert {'partitionKey': f'card/{card_id_21}', 'sortKey': '-'} in keys
    assert {'partitionKey': f'card/{card_id_11}', 'sortKey': '-'} in keys
    assert list(card_dynamo.generate_card_keys_by_comment(comment_id_2)) == [
        {'partitionKey': f'card/{card_id_12}', 'sortKey': '-'},
    ]


def test_generate_card_ids_by_notify_user_at(card_dynamo):
    # add a card with no user notification
    card_dynamo.add_card('coid', 'uoid', 'title', 'https://a.b')

    # generate no cards
    card_ids = list(card_dynamo.generate_card_ids_by_notify_user_at(pendulum.now('utc')))
    assert card_ids == []

    # add one card
    card_id_1 = str(uuid4())
    notify_user_at_1 = pendulum.now('utc')
    card_dynamo.add_card(card_id_1, 'uid', 'title1', 'https://a.b', notify_user_at=notify_user_at_1)

    # dont generate the card
    card_ids = list(
        card_dynamo.generate_card_ids_by_notify_user_at(notify_user_at_1 - pendulum.duration(microseconds=1))
    )
    assert card_ids == []

    # generate the card
    card_ids = list(card_dynamo.generate_card_ids_by_notify_user_at(notify_user_at_1))
    assert card_ids == [card_id_1]

    # add another card
    card_id_2 = str(uuid4())
    notify_user_at_2 = notify_user_at_1 + pendulum.duration(minutes=1)
    card_dynamo.add_card(card_id_2, 'uid2', 'title2', 'https://c.d', notify_user_at=notify_user_at_2)

    # don't generate either card
    card_ids = list(
        card_dynamo.generate_card_ids_by_notify_user_at(notify_user_at_1 - pendulum.duration(microseconds=1))
    )
    assert card_ids == []

    # generate just one card
    card_ids = list(
        card_dynamo.generate_card_ids_by_notify_user_at(notify_user_at_1 + pendulum.duration(seconds=1))
    )
    assert card_ids == [card_id_1]

    # generate both cards, check order
    card_ids = list(
        card_dynamo.generate_card_ids_by_notify_user_at(notify_user_at_1 + pendulum.duration(minutes=2))
    )
    assert card_ids == [card_id_1, card_id_2]


def test_generate_card_ids_by_notify_user_at_only_user_ids(card_dynamo):
    user_id_1, user_id_2, user_id_3 = [str(uuid4()), str(uuid4()), str(uuid4())]
    now = pendulum.now('utc')

    # add one card for the first one, two for the second, and three for the third
    card_id_10 = card_dynamo.add_card(str(uuid4()), user_id_1, 't', 'a', notify_user_at=now)['partitionKey'][5:]
    card_id_20 = card_dynamo.add_card(str(uuid4()), user_id_2, 't', 'a', notify_user_at=now)['partitionKey'][5:]
    card_id_21 = card_dynamo.add_card(str(uuid4()), user_id_2, 't', 'a', notify_user_at=now)['partitionKey'][5:]
    card_id_30 = card_dynamo.add_card(str(uuid4()), user_id_3, 't', 'a', notify_user_at=now)['partitionKey'][5:]
    card_id_31 = card_dynamo.add_card(str(uuid4()), user_id_3, 't', 'a', notify_user_at=now)['partitionKey'][5:]
    card_id_32 = card_dynamo.add_card(str(uuid4()), user_id_3, 't', 'a', notify_user_at=now)['partitionKey'][5:]

    # generate none of them, generate all of them based on time
    assert list(card_dynamo.generate_card_ids_by_notify_user_at(now - pendulum.duration(days=1))) == []
    assert sorted(list(card_dynamo.generate_card_ids_by_notify_user_at(now))) == sorted(
        [card_id_10, card_id_20, card_id_21, card_id_30, card_id_31, card_id_32]
    )

    # filter based on one user
    assert sorted(
        list(card_dynamo.generate_card_ids_by_notify_user_at(now, only_user_ids=[user_id_1]))
    ) == sorted([card_id_10])
    assert sorted(
        list(card_dynamo.generate_card_ids_by_notify_user_at(now, only_user_ids=[user_id_2]))
    ) == sorted([card_id_20, card_id_21])
    assert sorted(
        list(card_dynamo.generate_card_ids_by_notify_user_at(now, only_user_ids=[user_id_3]))
    ) == sorted([card_id_30, card_id_31, card_id_32])

    # filter based on two users
    assert sorted(
        list(card_dynamo.generate_card_ids_by_notify_user_at(now, only_user_ids=[user_id_1, user_id_2]))
    ) == sorted([card_id_10, card_id_20, card_id_21])
    assert sorted(
        list(card_dynamo.generate_card_ids_by_notify_user_at(now, only_user_ids=[user_id_2, user_id_3]))
    ) == sorted([card_id_20, card_id_21, card_id_30, card_id_31, card_id_32])
    assert sorted(
        list(card_dynamo.generate_card_ids_by_notify_user_at(now, only_user_ids=[user_id_3, user_id_1]))
    ) == sorted([card_id_30, card_id_31, card_id_32, card_id_10])

    # filter based on all three users
    assert sorted(
        list(
            card_dynamo.generate_card_ids_by_notify_user_at(now, only_user_ids=[user_id_1, user_id_2, user_id_3])
        )
    ) == sorted([card_id_10, card_id_20, card_id_21, card_id_30, card_id_31, card_id_32])
