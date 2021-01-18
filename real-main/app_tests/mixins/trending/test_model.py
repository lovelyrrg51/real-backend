import logging
import uuid
from decimal import Decimal

import pendulum
import pytest

from app.models.post.enums import PostType


@pytest.fixture
def user(user_manager, cognito_client):
    user_id, username = str(uuid.uuid4()), str(uuid.uuid4())[:8]
    cognito_client.create_user_pool_entry(user_id, username, verified_email=f'{username}@real.app')
    yield user_manager.create_cognito_only_user(user_id, username)


@pytest.fixture
def post(post_manager, user):
    post = post_manager.add_post(user, str(uuid.uuid4()), PostType.TEXT_ONLY, text='t')
    post.trending_delete()  # remove the auto-trending assigned to all posts
    post.refresh_trending_item()
    yield post


@pytest.mark.parametrize('model', pytest.lazy_fixture(['user', 'post']))
def test_increment_score_retry_count_exceeded(model):
    with pytest.raises(Exception, match=f'failed for item `{model.item_type}:{model.id}` after 3 tries'):
        model.trending_increment_score(retry_count=3)
    model.trending_increment_score(retry_count=2)  # no exception throw


@pytest.mark.parametrize('model', pytest.lazy_fixture(['user', 'post']))
def test_increment_score_add_new(model):
    now = pendulum.parse('2020-06-08T12:00:00Z')  # halfway through the day
    model.trending_increment_score(now=now)
    assert pendulum.parse(model.trending_item['createdAt']) == now
    assert pendulum.parse(model.trending_item['lastDeflatedAt']) == now
    assert model.trending_item['gsiA4SortKey'] == pytest.approx(Decimal(2 ** 0.5))


@pytest.mark.parametrize('model', pytest.lazy_fixture(['user', 'post']))
def test_increment_score_with_multiplier(model):
    now = pendulum.parse('2020-06-08T12:00:00Z')  # halfway through the day
    model.trending_increment_score(now=now, multiplier=0.5)
    assert pendulum.parse(model.trending_item['createdAt']) == now
    assert pendulum.parse(model.trending_item['lastDeflatedAt']) == now
    assert model.trending_item['gsiA4SortKey'] == pytest.approx(Decimal(0.5 * 2 ** 0.5))


@pytest.mark.parametrize('model', pytest.lazy_fixture(['user', 'post']))
def test_increment_score_add_new_race_condition(model, caplog):
    # sneak behind the model's back and add a trending
    assert model.trending_item is None
    created_at = pendulum.parse('2020-06-08T05:00:00Z')
    model.trending_dynamo.add(model.id, Decimal(2), now=created_at)

    # do the score icrement, verify
    now = pendulum.parse('2020-06-08T06:00:00Z')  # 1/4 through the day
    with caplog.at_level(logging.WARNING):
        model.trending_increment_score(now=now)
    assert len(caplog.records) == 1
    assert 'retry 1' in caplog.records[0].msg
    assert pendulum.parse(model.trending_item['createdAt']) == created_at
    assert pendulum.parse(model.trending_item['lastDeflatedAt']) == created_at
    assert model.trending_item['gsiA4SortKey'] == pytest.approx(Decimal(2 + 2 ** 0.25))


@pytest.mark.parametrize('model', pytest.lazy_fixture(['user', 'post']))
def test_increment_score_update_existing_basic(model):
    # create the trending item
    created_at = pendulum.parse('2020-06-08T12:00:00Z')  # 1/2 way through the day
    model.trending_increment_score(now=created_at)
    assert model.trending_item['gsiA4SortKey'] == pytest.approx(Decimal(2 ** 0.5))

    # udpate the score
    now = pendulum.parse('2020-06-08T18:00:00Z')  # 3/4 way through the day
    model.trending_increment_score(now=now)
    assert model.trending_item['gsiA4SortKey'] == pytest.approx(Decimal(2 ** 0.5 + 2 ** 0.75))

    # udpate the score, more than one day after last deflation
    now = pendulum.parse('2020-06-09T01:00:00Z')  # 25 hrs after
    model.trending_increment_score(now=now)
    assert model.trending_item['gsiA4SortKey'] == pytest.approx(Decimal(2 ** 0.5 + 2 ** 0.75 + 2 ** (25 / 24)))


@pytest.mark.parametrize('model', pytest.lazy_fixture(['user', 'post']))
def test_increment_score_update_existing_race_condition_deflation(model, caplog):
    # create the trending item
    created_at = pendulum.parse('2020-06-08T12:00:00Z')  # 1/2 way through the day
    model.trending_increment_score(now=created_at)
    score = model.trending_item['gsiA4SortKey']
    assert score == pytest.approx(Decimal(2 ** 0.5))

    # sneak behind our model's back and apply a deflation
    last_deflated_at = pendulum.parse('2020-06-09T01:00:00Z')
    new_score = score / 2
    model.trending_dynamo.deflate_score(model.id, score, new_score, created_at.date(), last_deflated_at)

    # update the score
    now = pendulum.parse('2020-06-09T02:00:00Z')
    with caplog.at_level(logging.WARNING):
        model.trending_increment_score(now=now)
    assert len(caplog.records) == 1
    assert 'retry 1' in caplog.records[0].msg
    assert model.trending_item['gsiA4SortKey'] == pytest.approx(new_score + Decimal(2 ** (1 / 12)))


@pytest.mark.parametrize('model', pytest.lazy_fixture(['user', 'post']))
def test_delete(model):
    assert model.trending_item is None

    # add a trending item for the model
    model.trending_dynamo.add(model.id, Decimal(1))
    model.refresh_trending_item()
    assert model.trending_item['partitionKey'].split('/') == [model.item_type, model.id]

    # delete the trending item
    model.trending_delete()
    assert model.trending_item is None
    assert model.refresh_trending_item().trending_item is None

    # delete the trending item when it doesn't exist
    model.trending_delete()
    assert model.trending_item is None
