import logging
import os
from decimal import Decimal

import amplitude

AMPLITUDE_API_KEY = os.environ.get('AMPLITUDE_API_KEY')
logger = logging.getLogger()


class AmplitudeClient:
    def __init__(self, api_key=AMPLITUDE_API_KEY):
        self.amplitude_logger = amplitude.AmplitudeLogger(api_key=api_key)

    def send_event(self, user_id, new_items, old_items=None):
        event_type = 'UPDATE_USER' if old_items else 'CREATE_USER'
        event_args = {
            'user_id': user_id,
            'event_type': event_type,
            "event_properties": {**new_items},
        }
        self.convert_decimal_to_float(event_args)
        event = self.amplitude_logger.create_event(**event_args)

        try:
            # send event to amplitude
            self.amplitude_logger.log_event(event)
        except Exception as err:
            logger.warning(str(err))
            return False

        return True

    def convert_decimal_to_float(self, source, parent_source=None, key=None):
        if type(source) == dict:
            for key in source:
                self.convert_decimal_to_float(source[key], source, key)
        elif type(source) == list:
            for item in source:
                self.convert_decimal_to_float(item, None, None)
        elif type(source) == Decimal:
            value = float(source)
            parent_source[key] = value
