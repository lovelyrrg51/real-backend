import logging
from collections import defaultdict

logger = logging.getLogger()


class DynamoDispatch:
    """
    A dispatcher that holds and allows searching over a catalogue of listener functions
    according to matching conditions which should trigger a call.
    """

    def __init__(self):
        self.listeners = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    def register(self, pk_prefix, sk_prefix, event_names, handler, attributes=None):
        """
        Register a handler.

        The `attributes` parameter, if provided, should be a dictionary of {name: default_value}.
        If `attributes` is present handler will only be called if at least one of the
        values of `attributes` have changed when applied to the old & new items.
        """
        for event_name in event_names:
            self.listeners[pk_prefix][sk_prefix][event_name].append(
                {'handler': handler, 'attributes': attributes}
            )

    def search(self, pk_prefix, sk_prefix, event_name, old_item, new_item):
        "Returns a set of matching listener functions"
        matches = []
        for listener in self.listeners[pk_prefix][sk_prefix][event_name]:
            if not listener['attributes']:
                matches.append(listener['handler'])
                continue
            for attr_name, attr_default in listener['attributes'].items():
                old_value = old_item.get(attr_name, attr_default)
                new_value = new_item.get(attr_name, attr_default)
                if old_value != new_value:
                    matches.append(listener['handler'])
                    break
        return matches
