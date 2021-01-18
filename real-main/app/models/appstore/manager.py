import logging

import pendulum

from .dynamo import AppStoreSubDynamo
from .enums import AppStoreSubscriptionStatus
from .exceptions import AppStoreException

logger = logging.getLogger()


class AppStoreManager:

    verification_period = pendulum.duration(hours=24)

    def __init__(self, clients, managers=None):
        managers = managers or {}
        managers['appstore'] = self

        self.clients = clients
        if 'appstore' in clients:
            self.appstore_client = self.clients['appstore']
        if 'dynamo' in clients:
            self.sub_dynamo = AppStoreSubDynamo(clients['dynamo'])

    def add_receipt(self, receipt, user_id):
        now = pendulum.now('utc')
        # purposely letting any app store client exceptions propogate up to top level so backend alerts fire
        parsed = self.appstore_client.verify_receipt(receipt, exclude_old_transactions=False)
        status = self.determine_status(parsed['latest_receipt_info'], now)
        self.sub_dynamo.add(
            original_transaction_id=parsed['original_transaction_id'],
            user_id=user_id,
            status=status,
            original_receipt=receipt,
            latest_receipt=parsed['latest_receipt'],
            latest_receipt_info=parsed['latest_receipt_info'],
            pending_renewal_info=parsed['pending_renewal_info'],
            next_verification_at=now + self.verification_period,
            now=now,
        )

    def determine_status(self, receipt_info, now):
        cancelled_at, expires_at = (
            pendulum.from_timestamp(float(receipt_info[x]) / 1000) if x in receipt_info else None
            for x in ('cancellation_date_ms', 'expires_date_ms')
        )
        if cancelled_at and cancelled_at <= now:
            return AppStoreSubscriptionStatus.CANCELLED
        if expires_at and expires_at <= now:
            return AppStoreSubscriptionStatus.EXPIRED
        return AppStoreSubscriptionStatus.ACTIVE

    def update_subscriptions(self, now=None):
        "Iterate through app store subscriptions and update our record with what appstore has"
        now = now or pendulum.now('utc')
        cnt = 0
        for key in self.sub_dynamo.generate_keys_to_reverify(now):
            original_transaction_id = key['partitionKey'].split('/')[1]
            receipt = self.sub_dynamo.client.get_item(key)['originalReceipt']
            self.update_subscription(original_transaction_id, receipt)
            cnt += 1
        return cnt

    def update_subscription(self, original_transaction_id, receipt):
        # makes sure appstore and our records agree on what the original transaction is
        parsed = self.appstore_client.verify_receipt(receipt, exclude_old_transactions=False)
        appstore_otid = parsed['original_transaction_id']
        if appstore_otid != original_transaction_id:
            raise AppStoreException(
                f'AppStore responded with a different original transaction id: `{appstore_otid}`'
                f' vs ours: `{original_transaction_id}`'
            )

        last_verification_at = pendulum.now('utc')
        status = self.determine_status(parsed['latest_receipt_info'], last_verification_at)
        self.sub_dynamo.update(
            original_transaction_id=parsed['original_transaction_id'],
            status=status,
            latest_receipt=parsed['latest_receipt'],
            latest_receipt_info=parsed['latest_receipt_info'],
            pending_renewal_info=parsed['pending_renewal_info'],
            last_verification_at=last_verification_at,
            next_verification_at=last_verification_at + self.verification_period,
        )

    def on_user_delete_delete_all_by_user(self, user_id, old_item):
        key_generator = self.sub_dynamo.generate_keys_by_user(user_id)
        self.sub_dynamo.client.batch_delete_items(key_generator)
