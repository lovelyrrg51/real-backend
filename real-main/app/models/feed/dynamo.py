import logging

logger = logging.getLogger()


class FeedDynamo:
    def __init__(self, dynamo_feed_client):
        self.feed_client = dynamo_feed_client

    def item(self, feed_user_id, post_item):
        return {
            'postId': post_item['postId'],
            'postedByUserId': post_item['postedByUserId'],
            'postedAt': post_item['postedAt'],
            'feedUserId': feed_user_id,
        }

    def add_posts_to_feed(self, feed_user_id, post_item_generator):
        item_generator = (self.item(feed_user_id, post_item) for post_item in post_item_generator)
        self.feed_client.batch_put_items(item_generator)

    def add_post_to_feeds(self, feed_user_id_generator, post_item):
        "Add the post to all the feeds of the generated user_ids, return a list of those user_ids"
        feed_user_ids = list(feed_user_id_generator)
        item_generator = (self.item(feed_user_id, post_item) for feed_user_id in feed_user_ids)
        self.feed_client.batch_put_items(item_generator)
        return feed_user_ids

    def delete_by_post_owner(self, feed_user_id, post_user_id):
        "Delete all feed items by `posted_by_user_id` from the feed of `feed_user_id`"
        key_generator = self.generate_keys_by_posted_by_user(feed_user_id, post_user_id)
        self.feed_client.batch_delete(key_generator)

    def delete_by_post(self, post_id):
        "Delete all feed items of `post_id`, return a list of affected user_ids"
        keys = list(self.generate_keys_by_post(post_id))
        feed_user_ids = [key['feedUserId'] for key in keys]
        self.feed_client.batch_delete(k for k in keys)
        return feed_user_ids

    def generate_items(self, feed_user_id):
        query_kwargs = {
            'KeyConditionExpression': 'feedUserId = :fuid',
            'ExpressionAttributeValues': {':fuid': feed_user_id},
            'IndexName': 'GSI-A1',
        }
        return self.feed_client.generate_all_query(query_kwargs)

    def generate_keys_by_post(self, post_id):
        query_kwargs = {
            'KeyConditionExpression': 'postId = :pid',
            'ExpressionAttributeValues': {':pid': post_id},
            'ProjectionExpression': 'postId, feedUserId',
        }
        return self.feed_client.generate_all_query(query_kwargs)

    def generate_keys_by_posted_by_user(self, feed_user_id, posted_by_user_id):
        query_kwargs = {
            'KeyConditionExpression': 'feedUserId = :fuid AND postedByUserId = :pbuid',
            'ExpressionAttributeValues': {':fuid': feed_user_id, ':pbuid': posted_by_user_id},
            'IndexName': 'GSI-A2',
            'ProjectionExpression': 'postId, feedUserId',
        }
        return self.feed_client.generate_all_query(query_kwargs)
