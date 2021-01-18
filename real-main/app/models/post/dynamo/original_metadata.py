import logging

logger = logging.getLogger()


class PostOriginalMetadataDynamo:
    def __init__(self, dynamo_client):
        self.client = dynamo_client

    def key(self, post_id):
        return {'partitionKey': f'post/{post_id}', 'sortKey': 'originalMetadata'}

    def get(self, post_id):
        return self.client.get_item(self.key(post_id))

    def delete(self, post_id):
        return self.client.delete_item(self.key(post_id))

    def add(self, post_id, original_metadata):
        item = {
            **self.key(post_id),
            'schemaVersion': 0,
            'originalMetadata': original_metadata,
        }
        return self.client.add_item({'Item': item})
