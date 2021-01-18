from uuid import uuid4

import pytest


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid4()), str(uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


user2 = user


@pytest.fixture
def screens(screen_manager, user):
    screen1 = screen_manager.init_screen(f'screen-1-{uuid4()}')
    screen2 = screen_manager.init_screen(f'screen-2-{uuid4()}')
    yield (screen1, screen2)


def test_record_views(screen_manager, user, user2, screens, caplog):
    screen1, screen2 = screens

    # check no views exist yet
    assert screen_manager.view_dynamo.get_view(screen1.id, user.id) is None
    assert screen_manager.view_dynamo.get_view(screen2.id, user.id) is None
    assert screen_manager.view_dynamo.get_view(screen1.id, user2.id) is None
    assert screen_manager.view_dynamo.get_view(screen2.id, user2.id) is None

    # user1 records one view, verify
    screen_manager.record_views([screen1.id], user.id)
    assert screen_manager.view_dynamo.get_view(screen1.id, user.id)['viewCount'] == 1
    assert screen_manager.view_dynamo.get_view(screen2.id, user.id) is None
    assert screen_manager.view_dynamo.get_view(screen1.id, user2.id) is None
    assert screen_manager.view_dynamo.get_view(screen2.id, user2.id) is None

    # user1 records some more views, verify
    screen_manager.record_views([screen2.id, screen1.id, screen1.id], user.id)
    assert screen_manager.view_dynamo.get_view(screen1.id, user.id)['viewCount'] == 3
    assert screen_manager.view_dynamo.get_view(screen2.id, user.id)['viewCount'] == 1
    assert screen_manager.view_dynamo.get_view(screen1.id, user2.id) is None
    assert screen_manager.view_dynamo.get_view(screen2.id, user2.id) is None

    # user2 records some views, verify
    screen_manager.record_views([screen2.id, screen2.id, screen1.id], user2.id)
    assert screen_manager.view_dynamo.get_view(screen1.id, user.id)['viewCount'] == 3
    assert screen_manager.view_dynamo.get_view(screen2.id, user.id)['viewCount'] == 1
    assert screen_manager.view_dynamo.get_view(screen1.id, user2.id)['viewCount'] == 1
    assert screen_manager.view_dynamo.get_view(screen2.id, user2.id)['viewCount'] == 2
