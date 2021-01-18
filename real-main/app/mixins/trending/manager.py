import logging

import pendulum

from .dynamo import TrendingDynamo
from .exceptions import TrendingDNEOrAttributeMismatch

logger = logging.getLogger()


class TrendingManagerMixin:

    score_inflation_per_day = 2

    min_count_to_keep = 10 * 1000
    min_score_to_keep = 0.5

    def __init__(self, clients, managers=None):
        super().__init__(clients, managers=managers)
        if 'dynamo' in clients:
            self.trending_dynamo = TrendingDynamo(self.item_type, clients['dynamo'])

    def trending_deflate(self, now=None):
        """
        Iterate over all trending items and deflate them.
        Returns a pair of integers: (total_items, deflated_items)
        """
        now = now or pendulum.now('utc')
        # iterates from lowest score upward, deflate and count each one
        total_count, deflated_count = 0, 0
        for item in self.trending_dynamo.generate_items():
            deflated = self.trending_deflate_item(item, now=now)
            deflated_count += int(deflated)
            total_count += 1
        return total_count, deflated_count

    def trending_deflate_item(self, trending_item, now=None, retry_count=0):
        item_id = trending_item['partitionKey'].split('/')[1]
        if retry_count > 2:
            raise Exception(
                f'trending_deflate_item() failed for item `{self.item_type}:{item_id}` after {retry_count} tries'
            )

        current_score = trending_item['gsiA4SortKey']
        if current_score == 0:
            logging.warning(f'Trending for item `{self.item_type}:{item_id}` already has score of zero')

        now = now or pendulum.now('utc')
        last_deflation_at = pendulum.parse(trending_item['lastDeflatedAt'])
        days_since_last_deflation = (now - last_deflation_at.start_of('day')).days
        if days_since_last_deflation < 1:
            logging.warning(f'Trending for item `{self.item_type}:{item_id}` has already been deflated today')
            return False

        new_score = current_score / (self.score_inflation_per_day ** days_since_last_deflation)

        try:
            self.trending_dynamo.deflate_score(item_id, current_score, new_score, last_deflation_at.date(), now)
        except TrendingDNEOrAttributeMismatch:
            logging.warning(f'Trending deflate failure, trying again for `{self.item_type}:{item_id}`')
            trending_item = self.trending_dynamo.get(item_id, strongly_consistent=True)
            return self.trending_deflate_item(trending_item, now=now, retry_count=retry_count + 1)
        return True

    def trending_delete_tail(self, total_count):
        max_to_delete = total_count - self.min_count_to_keep
        if max_to_delete <= 0:
            return 0

        deleted = 0
        for item in self.trending_dynamo.generate_items():
            item_id = item['partitionKey'].split('/')[1]
            current_score = item['gsiA4SortKey']
            if current_score >= self.min_score_to_keep:
                break
            try:
                self.trending_dynamo.delete(item_id, expected_score=current_score)
            except TrendingDNEOrAttributeMismatch:
                # race condition, the item must have recieved a boost in score
                logging.warning(f'Lost race condition, not deleting trending for `{self.item_type}:{item_id}`')
            else:
                deleted += 1
            if deleted >= max_to_delete:
                break

        return deleted
