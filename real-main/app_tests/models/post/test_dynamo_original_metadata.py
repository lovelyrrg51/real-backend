import pytest

from app.models.post.dynamo import PostOriginalMetadataDynamo


@pytest.fixture
def pom_dynamo(dynamo_client):
    yield PostOriginalMetadataDynamo(dynamo_client)


def test_transact_add_original_metadata_and_delete(pom_dynamo):
    post_id = 'pid'
    original_metadata = 'stringified json'
    assert pom_dynamo.get(post_id) is None

    # set the original metadata, verify
    item = pom_dynamo.add(post_id, original_metadata)
    assert pom_dynamo.get(post_id) == item
    assert item['originalMetadata'] == original_metadata
    assert item['schemaVersion'] == 0

    # verify can't set it again
    with pytest.raises(pom_dynamo.client.exceptions.ConditionalCheckFailedException):
        pom_dynamo.add(post_id, 'new value')
    assert pom_dynamo.get(post_id) == item

    # delete the original metadata, verify it disappears
    pom_dynamo.delete(post_id)
    assert pom_dynamo.get(post_id) is None

    # verify a no-op delete is ok
    pom_dynamo.delete(post_id)
    assert pom_dynamo.get(post_id) is None
