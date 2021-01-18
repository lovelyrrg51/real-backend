import logging

logger = logging.getLogger()


class PostImageDynamo:

    schema_version = 0

    def __init__(self, dynamo_client):
        self.client = dynamo_client

    def pk(self, post_id):
        return {'partitionKey': f'post/{post_id}', 'sortKey': 'image'}

    def get(self, post_id, strongly_consistent=False):
        return self.client.get_item(self.pk(post_id), ConsistentRead=strongly_consistent)

    def delete(self, post_id):
        return self.client.delete_item(self.pk(post_id))

    def set_initial_attributes(
        self, post_id, crop=None, image_format=None, original_format=None, taken_in_real=None
    ):
        attributes = {
            'crop': crop,
            'imageFormat': image_format,
            'originalFormat': original_format,
            'takenInReal': taken_in_real,
        }
        attributes = {k: v for k, v in attributes.items() if v is not None}
        if not attributes:
            return {}
        return self.client.set_attributes(self.pk(post_id), schemaVersion=self.schema_version, **attributes)

    def set_height_and_width(self, post_id, height, width):
        return self.client.set_attributes(
            self.pk(post_id), schemaVersion=self.schema_version, height=height, width=width
        )

    def set_colors(self, post_id, color_tuples):
        assert color_tuples, 'No support for deleting colors, yet'
        color_maps = [{'r': ct[0], 'g': ct[1], 'b': ct[2]} for ct in color_tuples]
        return self.client.set_attributes(self.pk(post_id), schemaVersion=self.schema_version, colors=color_maps)
