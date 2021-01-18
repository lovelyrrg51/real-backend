from unittest.mock import call, patch
from uuid import uuid4

import pendulum
import pytest

from app.models.card import templates
from app.models.post.enums import PostType


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user1 = user
user2 = user
user3 = user


@pytest.fixture
def chat_card_template(user):
    yield templates.ChatCardTemplate(user.id, chats_with_unviewed_messages_count=2)


@pytest.fixture
def requested_followers_card_template(user):
    yield templates.RequestedFollowersCardTemplate(user.id, requested_followers_count=3)


@pytest.fixture
def post(user, post_manager):
    yield post_manager.add_post(user, str(uuid4()), PostType.TEXT_ONLY, text='go go')


@pytest.fixture
def comment_card_template(user, post):
    yield templates.CommentCardTemplate(user.id, post.id, unviewed_comments_count=4)


@pytest.fixture
def post_likes_card_template(user, post):
    yield templates.PostLikesCardTemplate(user.id, post.id)


@pytest.fixture
def post_mention_card_template(user, post):
    yield templates.PostMentionCardTemplate(user.id, post=post)


@pytest.fixture
def post_views_card_template(user, post):
    yield templates.PostViewsCardTemplate(user.id, post.id)


post1 = post
post2 = post


@pytest.mark.parametrize(
    'template',
    pytest.lazy_fixture(['chat_card_template', 'comment_card_template', 'requested_followers_card_template']),
)
def test_add_or_update_card(user, template, card_manager):
    # verify starting state
    assert card_manager.get_card(template.card_id) is None

    # add the card, verify state
    before = pendulum.now('utc')
    card_manager.add_or_update_card(template)
    after = pendulum.now('utc')
    card = card_manager.get_card(template.card_id)
    assert card.id == template.card_id
    assert card.item['title'] == template.title
    assert card.item['action'] == template.action
    assert before < card.created_at < after
    if template.notify_user_after:
        assert card.notify_user_at == card.created_at + template.notify_user_after
    else:
        assert card.notify_user_at is None

    # try to add the card again with same title, verify no-op
    card_manager.add_or_update_card(template)
    new_card = card_manager.get_card(template.card_id)
    assert new_card.id == template.card_id
    assert new_card.item == card.item

    # update the card with a new title
    with patch.object(template, 'title', 'My new title'):
        card_manager.add_or_update_card(template)
    new_card = card_manager.get_card(template.card_id)
    assert new_card.id == template.card_id
    assert new_card.item.pop('title') == 'My new title'
    new_card.item['title'] = card.item['title']
    assert new_card.item == card.item


@pytest.mark.skip(reason="No cards with only_usernames set exist at the moment")
def test_add_or_update_card_with_only_usernames(user, template, card_manager):
    # verify starting state
    assert card_manager.get_card(template.card_id) is None

    # verify the only_usernames prevents us from ading the card
    assert card_manager.add_or_update_card(template) is None
    assert card_manager.get_card(template.card_id) is None

    # add the card, verify state
    before = pendulum.now('utc')
    with patch.object(template, 'only_usernames', (user.username,)):
        assert card_manager.add_or_update_card(template)
    after = pendulum.now('utc')
    card = card_manager.get_card(template.card_id)
    assert card.id == template.card_id
    assert card.item['title'] == template.title
    assert card.item['action'] == template.action
    assert before < card.created_at < after
    if template.notify_user_after:
        assert card.notify_user_at == card.created_at + template.notify_user_after
    else:
        assert card.notify_user_at is None

    # add the card again, verify no-op
    with patch.object(template, 'only_usernames', (user.username,)):
        assert card_manager.add_or_update_card(template)
    new_card = card_manager.get_card(template.card_id)
    assert new_card.id == template.card_id
    assert new_card.item['title'] == template.title
    assert new_card.item['action'] == template.action
    assert new_card.created_at == card.created_at

    # delete the card, verify it's gone
    card_manager.dynamo.delete_card(template.card_id)
    assert card_manager.get_card(template.card_id) is None

    # add the card again, this time with None for only_usernames
    with patch.object(template, 'only_usernames', None):
        assert card_manager.add_or_update_card(template)
    assert card_manager.get_card(template.card_id)


def test_comment_cards_are_per_post(user, card_manager, post1, post2):
    template1 = templates.CommentCardTemplate(user.id, post1.id, unviewed_comments_count=4)
    template2 = templates.CommentCardTemplate(user.id, post2.id, unviewed_comments_count=3)

    # verify starting state
    assert card_manager.get_card(template1.card_id) is None
    assert card_manager.get_card(template2.card_id) is None

    # add the card, verify state
    card_manager.add_or_update_card(template1)
    assert card_manager.get_card(template1.card_id)
    assert card_manager.get_card(template2.card_id) is None

    # add the other card, verify state and no conflict
    card_manager.add_or_update_card(template2)
    assert card_manager.get_card(template1.card_id)
    assert card_manager.get_card(template2.card_id)


def test_delete_by_post(card_manager, user1, user2, post1, post2, TestCardTemplate):
    # add a few cards, verify state
    kwargs = {'title': 't', 'action': 'a'}
    c10 = card_manager.add_or_update_card(TestCardTemplate(user1.id, **kwargs))
    c11 = card_manager.add_or_update_card(TestCardTemplate(user1.id, post_id=post1.id, **kwargs))
    c12 = card_manager.add_or_update_card(TestCardTemplate(user1.id, post_id=post2.id, **kwargs))
    c21 = card_manager.add_or_update_card(TestCardTemplate(user2.id, post_id=post1.id, **kwargs))
    c22 = card_manager.add_or_update_card(TestCardTemplate(user2.id, post_id=post2.id, **kwargs))
    for card in (c10, c11, c12, c21, c22):
        assert card_manager.get_card(card.id)

    # delete none, verify state
    card_manager.delete_by_post(str(uuid4()))
    card_manager.delete_by_post(str(uuid4()), user_id=user1.id)
    for card in (c10, c11, c12, c21, c22):
        assert card_manager.get_card(card.id)

    # delete all for one post, verify state
    card_manager.delete_by_post(post1.id)
    for card in (c10, c12, c22):
        assert card_manager.get_card(card.id)
    for card in (c11, c21):
        assert not card_manager.get_card(card.id)

    # delete post and user specific, verify state
    card_manager.delete_by_post(post2.id, user_id=user1.id)
    for card in (c10, c22):
        assert card_manager.get_card(card.id)
    for card in (c11, c12, c21):
        assert not card_manager.get_card(card.id)

    # delete for post, verify state
    card_manager.delete_by_post(post2.id)
    for card in (c10,):
        assert card_manager.get_card(card.id)
    for card in (c11, c12, c21, c22):
        assert not card_manager.get_card(card.id)


def test_delete_by_comment(card_manager, user1, user2, TestCardTemplate):
    # add a few cards, verify state
    kwargs = {'title': 't', 'action': 'a'}
    comment_id_1 = str(uuid4())
    comment_id_2 = str(uuid4())
    c10 = card_manager.add_or_update_card(TestCardTemplate(user1.id, **kwargs))
    c11 = card_manager.add_or_update_card(TestCardTemplate(user1.id, comment_id=comment_id_1, **kwargs))
    c12 = card_manager.add_or_update_card(TestCardTemplate(user1.id, comment_id=comment_id_2, **kwargs))
    c21 = card_manager.add_or_update_card(TestCardTemplate(user2.id, comment_id=comment_id_1, **kwargs))
    c22 = card_manager.add_or_update_card(TestCardTemplate(user2.id, comment_id=comment_id_2, **kwargs))
    for card in (c10, c11, c12, c21, c22):
        assert card_manager.get_card(card.id)

    # delete none, verify state
    card_manager.delete_by_comment(str(uuid4()))
    for card in (c10, c11, c12, c21, c22):
        assert card_manager.get_card(card.id)

    # delete all for one comment, verify state
    card_manager.delete_by_comment(comment_id_1)
    for card in (c10, c12, c22):
        assert card_manager.get_card(card.id)
    for card in (c11, c21):
        assert not card_manager.get_card(card.id)

    # delete for post, verify state
    card_manager.delete_by_comment(comment_id_2)
    for card in (c10,):
        assert card_manager.get_card(card.id)
    for card in (c11, c12, c21, c22):
        assert not card_manager.get_card(card.id)


def test_notify_users(card_manager, pinpoint_client, user, user2, TestCardTemplate):
    # configure mock to claim all apns-sending attempts succeeded
    pinpoint_client.configure_mock(**{'send_user_apns.return_value': True})
    now = pendulum.now('utc')

    # add a card with a notification in the far future
    card1 = card_manager.add_or_update_card(
        TestCardTemplate(user.id, title='t1', action='a1', notify_user_after=pendulum.duration(hours=1)),
        now=now,
    )
    assert card1.notify_user_at == now + pendulum.duration(hours=1)

    # run notificiations, verify none sent and no db changes
    cnts = card_manager.notify_users()
    assert cnts == (0, 0)
    assert pinpoint_client.mock_calls == []
    assert card1.item == card1.refresh_item().item

    # add another card with a notification in the immediate future
    card2 = card_manager.add_or_update_card(
        TestCardTemplate(user.id, title='t2', action='a2', notify_user_after=pendulum.duration(seconds=2)),
        now=now,
    )
    assert card2.notify_user_at == now + pendulum.duration(seconds=2)

    # run notificiations, verify none sent and no db changes
    cnts = card_manager.notify_users()
    assert cnts == (0, 0)
    assert pinpoint_client.mock_calls == []
    assert card1.item == card1.refresh_item().item
    assert card2.item == card2.refresh_item().item

    # add another card with a notification in the immediate past
    card3 = card_manager.add_or_update_card(
        TestCardTemplate(user.id, title='t3', action='a3', notify_user_after=pendulum.duration()), now=now
    )
    assert card3.notify_user_at == now

    # run notificiations, verify one sent
    cnts = card_manager.notify_users()
    assert cnts == (1, 1)
    assert pinpoint_client.mock_calls == [
        call.send_user_apns(user.id, 'a3', 't3', body=None),
    ]
    assert card1.item == card1.refresh_item().item
    assert card2.item == card2.refresh_item().item
    assert card3.refresh_item().notify_user_at is None

    # two cards with a notification in past
    card4 = card_manager.add_or_update_card(
        TestCardTemplate(user2.id, title='t4', action='a4', notify_user_after=pendulum.duration(seconds=-1)),
        now=now,
    )
    card5 = card_manager.add_or_update_card(
        TestCardTemplate(
            user.id, title='t5', action='a5', notify_user_after=pendulum.duration(hours=-1), sub_title='s'
        ),
        now=now,
    )
    assert card4.notify_user_at == now + pendulum.duration(seconds=-1)
    assert card5.notify_user_at == now + pendulum.duration(hours=-1)

    # run notificiations, verify both sent
    pinpoint_client.reset_mock()
    cnts = card_manager.notify_users()
    assert cnts == (2, 2)
    assert pinpoint_client.mock_calls == [
        call.send_user_apns(user.id, 'a5', 't5', body='s'),
        call.send_user_apns(user2.id, 'a4', 't4', body=None),
    ]
    assert card1.item == card1.refresh_item().item
    assert card2.item == card2.refresh_item().item
    assert card4.refresh_item().notify_user_at is None
    assert card5.refresh_item().notify_user_at is None


def test_notify_users_failed_notification(card_manager, pinpoint_client, user, TestCardTemplate):
    # add card with a notification in the immediate past
    now = pendulum.now('utc')
    card = card_manager.add_or_update_card(
        TestCardTemplate(user.id, title='t', action='a', notify_user_after=pendulum.duration()),
        now=now,
    )
    assert card.notify_user_at == now

    # configure our mock to report a failed message send
    pinpoint_client.configure_mock(**{'send_user_apns.return_value': False})

    # run notificiations, verify attempted send and correct DB changes upon failure
    cnts = card_manager.notify_users()
    assert cnts == (1, 0)
    assert pinpoint_client.mock_calls == [call.send_user_apns(user.id, 'a', 't', body=None)]
    org_item = card.item
    card.refresh_item()
    assert 'gsiK1PartitionKey' not in card.item
    assert 'gsiK1SortKey' not in card.item
    assert org_item.pop('gsiK1PartitionKey')
    assert org_item.pop('gsiK1SortKey')
    assert card.item == org_item


def test_notify_users_only_usernames(card_manager, pinpoint_client, user, user2, user3, TestCardTemplate):
    # configure mock to claim all apns-sending attempts succeeded
    pinpoint_client.configure_mock(**{'send_user_apns.return_value': True})

    # add one notification for each user in immediate past, verify they're there
    card1 = card_manager.add_or_update_card(
        TestCardTemplate(user.id, title='t1', action='a1', notify_user_after=pendulum.duration(seconds=-2)),
    )
    card2 = card_manager.add_or_update_card(
        TestCardTemplate(user2.id, title='t2', action='a2', notify_user_after=pendulum.duration(seconds=-1)),
    )
    card3 = card_manager.add_or_update_card(
        TestCardTemplate(user3.id, title='t3', action='a3', notify_user_after=pendulum.duration()),
    )
    assert card1.refresh_item().notify_user_at
    assert card2.refresh_item().notify_user_at
    assert card3.refresh_item().notify_user_at

    # run notificiations for just two of the users, verify just those two sent
    pinpoint_client.reset_mock()
    cnts = card_manager.notify_users(only_usernames=[user.username, user3.username])
    assert cnts == (2, 2)
    assert pinpoint_client.mock_calls == [
        call.send_user_apns(user.id, 'a1', 't1', body=None),
        call.send_user_apns(user3.id, 'a3', 't3', body=None),
    ]
    assert card1.refresh_item().notify_user_at is None
    assert card2.refresh_item().notify_user_at
    assert card3.refresh_item().notify_user_at is None

    # re-add those cards for which we just sent notificaitons
    card_manager.dynamo.delete_card(card1.id)
    card_manager.dynamo.delete_card(card3.id)
    card1 = card_manager.add_or_update_card(
        TestCardTemplate(user.id, title='t1', action='a1', notify_user_after=pendulum.duration(seconds=-2))
    )
    card3 = card_manager.add_or_update_card(
        TestCardTemplate(user3.id, title='t3', action='a3', notify_user_after=pendulum.duration())
    )
    assert card1.refresh_item().notify_user_at
    assert card3.refresh_item().notify_user_at

    # run notificiations for just one of the user, verify just that one sent
    pinpoint_client.reset_mock()
    cnts = card_manager.notify_users(only_usernames=[user2.username])
    assert cnts == (1, 1)
    assert pinpoint_client.mock_calls == [
        call.send_user_apns(user2.id, 'a2', 't2', body=None),
    ]
    assert card1.refresh_item().notify_user_at
    assert card2.refresh_item().notify_user_at is None
    assert card3.refresh_item().notify_user_at

    # re-add a cards for which we just sent notificaitons
    card_manager.dynamo.delete_card(card2.id)
    card2 = card_manager.add_or_update_card(
        TestCardTemplate(user2.id, title='t2', action='a2', notify_user_after=pendulum.duration(seconds=-1))
    )
    assert card2.refresh_item().notify_user_at

    # run notificiations for no users, verify none sent
    pinpoint_client.reset_mock()
    cnts = card_manager.notify_users(only_usernames=[])
    assert cnts == (0, 0)
    assert pinpoint_client.mock_calls == []
    assert card1.refresh_item().notify_user_at
    assert card2.refresh_item().notify_user_at
    assert card3.refresh_item().notify_user_at

    # run notificiations for all users, verify all sent
    pinpoint_client.reset_mock()
    cnts = card_manager.notify_users()
    assert cnts == (3, 3)
    assert pinpoint_client.mock_calls == [
        call.send_user_apns(user.id, 'a1', 't1', body=None),
        call.send_user_apns(user2.id, 'a2', 't2', body=None),
        call.send_user_apns(user3.id, 'a3', 't3', body=None),
    ]
    assert card1.refresh_item().notify_user_at is None
    assert card2.refresh_item().notify_user_at is None
    assert card3.refresh_item().notify_user_at is None
