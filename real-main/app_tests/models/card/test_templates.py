import re
from uuid import uuid4

import pytest

from app.models.card import templates
from app.models.post.enums import PostType


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def post(post_manager, user):
    yield post_manager.add_post(user, str(uuid4()), PostType.TEXT_ONLY, text='go go')


@pytest.fixture
def comment(comment_manager, user2, post):
    yield comment_manager.add_comment(str(uuid4()), post.id, user2.id, 'lore ipsum')


user2 = user
post1 = post
post2 = post


def test_post_views_card_template(user, post):
    card_id = templates.PostViewsCardTemplate.get_card_id(user.id, post.id)
    assert card_id.split(':') == [user.id, 'POST_VIEWS', post.id]

    template = templates.PostViewsCardTemplate(user.id, post.id)
    assert template.card_id == card_id
    assert template.user_id == user.id
    assert template.action == f'https://real.app/user/{user.id}/post/{post.id}/views'
    assert template.title == 'You have new views'
    assert not template.only_usernames
    assert template.post_id == post.id
    assert not template.comment_id


def test_comment_mention_card_template(user, comment):
    card_id = templates.CommentMentionCardTemplate.get_card_id(user.id, comment.id)
    assert card_id.split(':') == [user.id, 'COMMENT_MENTION', comment.id]

    template = templates.CommentMentionCardTemplate(user.id, comment)
    assert template.card_id == card_id
    assert template.user_id == user.id
    assert template.action.split('/') == [
        'https:',
        '',
        'real.app',
        'user',
        comment.post.user_id,
        'post',
        comment.post_id,
        'comments',
        comment.id,
    ]
    assert re.match(r'@.* mentioned you in a comment', template.title)
    assert comment.user.username in template.title
    assert not template.only_usernames
    assert template.post_id == comment.post_id
    assert template.comment_id == comment.id


def test_post_mention_card_template(user, post):
    card_id = templates.PostMentionCardTemplate.get_card_id(user.id, post.id)
    assert card_id.split(':') == [user.id, 'POST_MENTION', post.id]

    template = templates.PostMentionCardTemplate(user.id, post)
    assert template.card_id == card_id
    assert template.user_id == user.id
    assert template.action == f'https://real.app/user/{post.user_id}/post/{post.id}'
    assert re.match(r'@.* tagged you in a post', template.title)
    assert post.user.username in template.title
    assert not template.only_usernames
    assert template.post_id == post.id
    assert not template.comment_id


def test_post_repost_card_template(user, post, user2):
    card_id = templates.PostRepostCardTemplate.get_card_id(user2.id, post.id)
    assert card_id.split(':') == [user2.id, 'POST_REPOST', post.id]

    template = templates.PostRepostCardTemplate(user2.id, post)
    assert template.card_id == card_id
    assert template.user_id == user2.id
    assert template.action == f'https://real.app/user/{post.user_id}/post/{post.id}'
    assert re.match(r'@.* reposted one of your posts', template.title)
    assert post.user.username in template.title
    assert not template.only_usernames
    assert template.post_id == post.id
    assert not template.comment_id


def test_post_likes_card_template(user, post):
    card_id = templates.PostLikesCardTemplate.get_card_id(user.id, post.id)
    assert card_id.split(':') == [user.id, 'POST_LIKES', post.id]

    template = templates.PostLikesCardTemplate(user.id, post.id)
    assert template.card_id == card_id
    assert template.user_id == user.id
    assert template.action == f'https://real.app/user/{user.id}/post/{post.id}/likes'
    assert template.title == 'You have new likes'
    assert not template.only_usernames
    assert template.post_id == post.id
    assert not template.comment_id


def test_comment_card_template(user, post):
    card_id = templates.CommentCardTemplate.get_card_id(user.id, post.id)
    assert card_id.split(':') == [user.id, 'COMMENT_ACTIVITY', post.id]

    template = templates.CommentCardTemplate(user.id, post.id, 1)
    assert template.card_id == card_id
    assert template.user_id == user.id
    assert template.action == f'https://real.app/user/{user.id}/post/{post.id}/comments'
    assert not template.only_usernames
    assert template.post_id == post.id
    assert not template.comment_id


def test_comment_card_template_titles(user, post):
    template = templates.CommentCardTemplate(user.id, post.id, 1)
    assert template.title == 'You have 1 new comment'

    template = templates.CommentCardTemplate(user.id, post.id, 2)
    assert template.title == 'You have 2 new comments'

    template = templates.CommentCardTemplate(user.id, post.id, 42)
    assert template.title == 'You have 42 new comments'


def test_comment_card_templates_are_per_post(user, post1, post2):
    assert (
        templates.CommentCardTemplate(user.id, post1.id, 1).card_id
        == templates.CommentCardTemplate(user.id, post1.id, 1).card_id
    )
    assert (
        templates.CommentCardTemplate(user.id, post1.id, 1).card_id
        != templates.CommentCardTemplate(user.id, post2.id, 1).card_id
    )


def test_chat_card_template(user):
    card_id = templates.ChatCardTemplate.get_card_id(user.id)
    assert card_id.split(':') == [user.id, 'CHAT_ACTIVITY']

    template = templates.ChatCardTemplate(user.id, 1)
    assert template.card_id == card_id
    assert template.user_id == user.id
    assert template.action == 'https://real.app/chat/'
    assert not template.only_usernames
    assert not template.post_id
    assert not template.comment_id


def test_chat_card_template_titles(user):
    template = templates.ChatCardTemplate(user.id, 1)
    assert template.title == 'You have 1 chat with new messages'

    template = templates.ChatCardTemplate(user.id, 2)
    assert template.title == 'You have 2 chats with new messages'

    template = templates.ChatCardTemplate(user.id, 42)
    assert template.title == 'You have 42 chats with new messages'


def test_requested_followers_card_template(user):
    card_id = templates.RequestedFollowersCardTemplate.get_card_id(user.id)
    assert card_id.split(':') == [user.id, 'REQUESTED_FOLLOWERS']

    template = templates.RequestedFollowersCardTemplate(user.id, 1)
    assert template.card_id == card_id
    assert template.user_id == user.id
    assert template.action == 'https://real.app/chat/'
    assert not template.only_usernames
    assert not template.post_id
    assert not template.comment_id


def test_requested_followers_card_template_titles(user):
    template = templates.RequestedFollowersCardTemplate(user.id, 1)
    assert template.title == 'You have 1 pending follow request'

    template = templates.RequestedFollowersCardTemplate(user.id, 2)
    assert template.title == 'You have 2 pending follow requests'

    template = templates.RequestedFollowersCardTemplate(user.id, 42)
    assert template.title == 'You have 42 pending follow requests'


def test_contact_joined_card_template(user, user2):
    card_id = templates.ContactJoinedCardTemplate.get_card_id(user.id, user2.id)
    assert card_id.split(':') == [user.id, 'CONTACT_JOINED', user2.id]

    template = templates.ContactJoinedCardTemplate(user.id, user2.id, user2.username)
    assert template.card_id == card_id
    assert template.user_id == user.id
    assert template.action == f'https://real.app/user/{user2.id}'
    assert user2.username in template.title
    assert ' joined REAL' in template.title
    assert not template.only_usernames
    assert not template.post_id
    assert not template.comment_id
