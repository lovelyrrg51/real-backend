# https://developer.apple.com/documentation/appstorereceipts
import requests


class AppStoreClientException(Exception):
    pass


class AppStoreClient:
    def __init__(self, appstore_params_getter):
        self.appstore_params_getter = appstore_params_getter
        self.url_production = 'https://buy.itunes.apple.com/verifyReceipt'
        self.url_sandbox = 'https://sandbox.itunes.apple.com/verifyReceipt'

    @property
    def bundle_id(self):
        if not hasattr(self, '_appstore_params'):
            self._appstore_params = self.appstore_params_getter()
        return self._appstore_params['bundleId']

    @property
    def shared_secret(self):
        if not hasattr(self, '_appstore_params'):
            self._appstore_params = self.appstore_params_getter()
        return self._appstore_params['sharedSecret']

    def verify_receipt(self, receipt_data_b64, exclude_old_transactions=False):
        req_body = {
            'password': self.shared_secret,
            'receipt-data': receipt_data_b64,
            'exclude-old-transactions': exclude_old_transactions,
        }
        # per Apple recommendation, we first attempt to validate with production
        # and then attempt with staging only upon receiving a 21007 status code from production
        # https://developer.apple.com/documentation/appstorereceipts/verifyreceipt#discussion
        resp = self.do_appstore_post(self.url_production, req_body) or self.do_appstore_post(
            self.url_sandbox, req_body
        )
        return self.parse_response(resp)

    def do_appstore_post(self, url, req_body):
        """
        For a valid receipt, return the response body from apple.
        For a receipt that is for the other environment, return None.
        For all other apple statuses, raise an exception.
        """
        receipt = req_body['receipt-data']
        resp_body = requests.post(url, json=req_body).json()

        # https://developer.apple.com/documentation/appstorereceipts/status
        status = resp_body.get('status')
        if status in (21007, 21008):
            return None
        if status != 0:
            raise AppStoreClientException(
                f'AppStore `{url}` responded with status `{status}` for receipt `{receipt}`'
            )

        bundle_id = resp_body['receipt']['bundle_id']
        if bundle_id != self.bundle_id:
            raise AppStoreClientException(
                f'AppStore `{url}` responded with bundle id `{bundle_id}` for receipt `{receipt}`'
            )

        return resp_body

    def parse_response(self, resp_body):
        "Pull out and isolate the fields we care about"
        return {
            'latest_receipt': resp_body['latest_receipt'],
            'latest_receipt_info': resp_body['latest_receipt_info'][-1],
            'original_transaction_id': resp_body['latest_receipt_info'][-1]['original_transaction_id'],
            'pending_renewal_info': resp_body['pending_renewal_info'],
        }
