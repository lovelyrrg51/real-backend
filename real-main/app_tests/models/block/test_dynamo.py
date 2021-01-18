import pendulum
import pytest

from app.models.block import exceptions
from app.models.block.dynamo import BlockDynamo


@pytest.fixture
def block_dynamo(dynamo_client):
    yield BlockDynamo(dynamo_client)


def test_add_block(block_dynamo):
    blocker_user_id = 'blocker-user-id'
    blocked_user_id = 'blocked-used-id'
    now = pendulum.now('utc')
    resp = block_dynamo.add_block(blocker_user_id, blocked_user_id, now=now)
    assert resp == {
        'schemaVersion': 0,
        'partitionKey': f'user/{blocked_user_id}',
        'sortKey': f'blocker/{blocker_user_id}',
        'gsiA1PartitionKey': f'block/{blocker_user_id}',
        'gsiA1SortKey': now.to_iso8601_string(),
        'gsiA2PartitionKey': f'block/{blocked_user_id}',
        'gsiA2SortKey': now.to_iso8601_string(),
        'blockerUserId': blocker_user_id,
        'blockedUserId': blocked_user_id,
        'blockedAt': now.to_iso8601_string(),
    }


def test_block_basic_crud_cycle(block_dynamo):
    blocker_user_id = 'blocker-user-id'
    blocked_user_id = 'blocked-used-id'

    # check the block is not there
    resp = block_dynamo.get_block(blocker_user_id, blocked_user_id)
    assert resp is None

    # add the block
    resp = block_dynamo.add_block(blocker_user_id, blocked_user_id)
    assert resp['blockerUserId'] == blocker_user_id
    assert resp['blockedUserId'] == blocked_user_id
    blocked_at = resp['blockedAt']

    # check the block is there
    resp = block_dynamo.get_block(blocker_user_id, blocked_user_id)
    assert resp['blockerUserId'] == blocker_user_id
    assert resp['blockedUserId'] == blocked_user_id
    assert resp['blockedAt'] == blocked_at

    # delete the block
    resp = block_dynamo.delete_block(blocker_user_id, blocked_user_id)
    assert resp['blockerUserId'] == blocker_user_id
    assert resp['blockedUserId'] == blocked_user_id
    assert resp['blockedAt'] == blocked_at

    # check the block is no longer there
    resp = block_dynamo.get_block(blocker_user_id, blocked_user_id)
    assert resp is None


def test_add_block_already_exists(block_dynamo):
    blocker_user_id = 'blocker-user-id'
    blocked_user_id = 'blocked-used-id'

    # add the block
    resp = block_dynamo.add_block(blocker_user_id, blocked_user_id)
    assert resp['blockerUserId'] == blocker_user_id
    assert resp['blockedUserId'] == blocked_user_id

    # try to add the block again
    with pytest.raises(exceptions.AlreadyBlocked):
        block_dynamo.add_block(blocker_user_id, blocked_user_id)


def test_delete_block_doesnt_exist(block_dynamo):
    blocker_user_id = 'blocker-user-id'
    blocked_user_id = 'blocked-used-id'
    assert block_dynamo.delete_block(blocker_user_id, blocked_user_id) is None


def test_generate_blocks_by_blocker(block_dynamo):
    blocker_user_id = 'blocker-user-id'
    blocked1_user_id = 'b1-user-id'
    blocked2_user_id = 'b2-user-id'

    # generate our current blocks
    blocks = list(block_dynamo.generate_blocks_by_blocker(blocker_user_id))
    assert len(blocks) == 0

    # block them both
    block_dynamo.add_block(blocker_user_id, blocked1_user_id)
    block_dynamo.add_block(blocker_user_id, blocked2_user_id)

    # check generation of blocks
    blocks = list(block_dynamo.generate_blocks_by_blocker(blocker_user_id))
    assert len(blocks) == 2
    assert blocks[0]['blockedUserId'] == blocked1_user_id
    assert blocks[1]['blockedUserId'] == blocked2_user_id


def test_generate_blocks_by_blocked(block_dynamo):
    blocked_user_id = 'blocker-user-id'
    blocker1_user_id = 'b1-user-id'
    blocker2_user_id = 'b2-user-id'

    # generate our current blocks
    blocks = list(block_dynamo.generate_blocks_by_blocked(blocked_user_id))
    assert len(blocks) == 0

    # block them both
    block_dynamo.add_block(blocker1_user_id, blocked_user_id)
    block_dynamo.add_block(blocker2_user_id, blocked_user_id)

    # check generation of blocks
    blocks = list(block_dynamo.generate_blocks_by_blocked(blocked_user_id))
    assert len(blocks) == 2
    assert blocks[0]['blockerUserId'] == blocker1_user_id
    assert blocks[1]['blockerUserId'] == blocker2_user_id


def test_delete_all_blocks_by_user(block_dynamo):
    blocker_user_id = 'blocker-user-id'
    blocked1_user_id = 'b1-user-id'
    blocked2_user_id = 'b2-user-id'

    # block them both
    block_dynamo.add_block(blocker_user_id, blocked1_user_id)
    block_dynamo.add_block(blocker_user_id, blocked2_user_id)

    # check blocks exist
    blocks = list(block_dynamo.generate_blocks_by_blocker(blocker_user_id))
    assert len(blocks) == 2
    assert blocks[0]['blockedUserId'] == blocked1_user_id
    assert blocks[1]['blockedUserId'] == blocked2_user_id

    # unblock, check they disappeared
    block_dynamo.delete_all_blocks_by_user(blocker_user_id)
    assert list(block_dynamo.generate_blocks_by_blocker(blocker_user_id)) == []


def test_delete_all_blocks_of_user(block_dynamo):
    blocked_user_id = 'blocked-user-id'
    blocker1_user_id = 'b1-user-id'
    blocker2_user_id = 'b2-user-id'

    # they both block
    block_dynamo.add_block(blocker1_user_id, blocked_user_id)
    block_dynamo.add_block(blocker2_user_id, blocked_user_id)

    # check blocks exist
    blocks = list(block_dynamo.generate_blocks_by_blocked(blocked_user_id))
    assert len(blocks) == 2
    assert blocks[0]['blockerUserId'] == blocker1_user_id
    assert blocks[1]['blockerUserId'] == blocker2_user_id

    # unblock, check they disappeared
    block_dynamo.delete_all_blocks_of_user(blocked_user_id)
    assert list(block_dynamo.generate_blocks_by_blocked(blocked_user_id)) == []
