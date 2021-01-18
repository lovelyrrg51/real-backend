import logging

import pendulum

from .exceptions import AppStoreSubAlreadyExists

logger = logging.getLogger()


class AppStoreSubDynamo:
    def __init__(self, dynamo_client):
        self.client = dynamo_client

    def key(self, original_transaction_id):
        return {'partitionKey': f'appStoreSub/{original_transaction_id}', 'sortKey': '-'}

    def get(self, original_transaction_id, strongly_consistent=False):
        return self.client.get_item(self.key(original_transaction_id), ConsistentRead=strongly_consistent)

    def add(
        self,
        original_transaction_id,
        user_id,
        status,
        original_receipt,
        latest_receipt,
        latest_receipt_info,
        pending_renewal_info,
        next_verification_at,
        now=None,
    ):
        now = now or pendulum.now('utc')
        item = {
            **self.key(original_transaction_id),
            'schemaVersion': 0,
            'userId': user_id,
            'status': status,
            'createdAt': now.to_iso8601_string(),
            'lastVerificationAt': now.to_iso8601_string(),
            'originalReceipt': original_receipt,
            'latestReceipt': latest_receipt,
            'latestReceiptInfo': latest_receipt_info,
            'pendingRenewalInfo': pending_renewal_info,
            'gsiA1PartitionKey': f'appStoreSub/{user_id}',
            'gsiA1SortKey': now.to_iso8601_string(),
            'gsiK1PartitionKey': 'appStoreSub',
            'gsiK1SortKey': next_verification_at.to_iso8601_string(),
        }
        try:
            self.client.add_item({'Item': item})
        except self.client.exceptions.ConditionalCheckFailedException as err:
            raise AppStoreSubAlreadyExists(original_transaction_id) from err
        return item

    def update(
        self,
        original_transaction_id,
        status,
        latest_receipt,
        latest_receipt_info,
        pending_renewal_info,
        last_verification_at,
        next_verification_at,
    ):
        query_kwargs = {
            'Key': self.key(original_transaction_id),
            'UpdateExpression': 'SET #s = :s, #lva = :lva, #lr = :lr, #lri = :lri, #pri = :pri, #sk = :sk',
            'ExpressionAttributeValues': {
                ':s': status,
                ':lva': last_verification_at.to_iso8601_string(),
                ':lr': latest_receipt,
                ':lri': latest_receipt_info,
                ':pri': pending_renewal_info,
                ':sk': next_verification_at.to_iso8601_string(),
            },
            'ExpressionAttributeNames': {
                '#s': 'status',
                '#lva': 'lastVerificationAt',
                '#lr': 'latestReceipt',
                '#lri': 'latestReceiptInfo',
                '#pri': 'pendingRenewalInfo',
                '#sk': 'gsiK1SortKey',
            },
        }
        return self.client.update_item(query_kwargs)

    def generate_keys_to_reverify(self, now):
        query_kwargs = {
            'KeyConditionExpression': 'gsiK1PartitionKey = :pk AND gsiK1SortKey <= :sk_max',
            'ExpressionAttributeValues': {':pk': 'appStoreSub', ':sk_max': now.to_iso8601_string()},
            'ProjectionExpression': 'partitionKey, sortKey',
            'IndexName': 'GSI-K1',
        }
        return self.client.generate_all_query(query_kwargs)

    def generate_keys_by_user(self, user_id):
        query_kwargs = {
            'KeyConditionExpression': 'gsiA1PartitionKey = :pk',
            'ExpressionAttributeValues': {':pk': f'appStoreSub/{user_id}'},
            'ProjectionExpression': 'partitionKey, sortKey',
            'IndexName': 'GSI-A1',
        }
        return self.client.generate_all_query(query_kwargs)
