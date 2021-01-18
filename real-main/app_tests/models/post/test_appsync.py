import uuid

import pytest

from app.models.post.appsync import PostAppSync
from app.models.post.enums import PostNotificationType, PostType


@pytest.fixture
def post_appsync(appsync_client):
    yield PostAppSync(appsync_client)


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def completed_post(post_manager, user, image_data_b64):
    post_id = str(uuid.uuid4())
    yield post_manager.add_post(user, post_id, PostType.IMAGE, image_input={'imageData': image_data_b64})


def test_trigger_notification_completed(post_appsync, user, completed_post, appsync_client):
    # check starting state
    assert completed_post.item['isVerified'] is True
    appsync_client.reset_mock()

    # trigger, check client was called correctly
    post_appsync.trigger_notification(PostNotificationType.COMPLETED, completed_post)
    assert len(appsync_client.mock_calls) == 1
    assert len(appsync_client.send.call_args.kwargs) == 0
    assert len(appsync_client.send.call_args.args) == 2
    assert 'triggerPostNotification' in str(appsync_client.send.call_args.args[0])
    assert appsync_client.send.call_args.args[1] == {
        'input': {
            'userId': user.id,
            'type': 'COMPLETED',
            'postId': completed_post.id,
            'postStatus': 'COMPLETED',
            'isVerified': True,
        }
    }

    # clear client mock state and mark the post failed
    appsync_client.reset_mock()
    completed_post.item['isVerified'] = False

    # trigger, check client was called correctly
    post_appsync.trigger_notification(PostNotificationType.COMPLETED, completed_post)
    assert len(appsync_client.mock_calls) == 1
    assert len(appsync_client.send.call_args.kwargs) == 0
    assert len(appsync_client.send.call_args.args) == 2
    assert 'triggerPostNotification' in str(appsync_client.send.call_args.args[0])
    assert appsync_client.send.call_args.args[1] == {
        'input': {
            'userId': user.id,
            'type': 'COMPLETED',
            'postId': completed_post.id,
            'postStatus': 'COMPLETED',
            'isVerified': False,
        }
    }
