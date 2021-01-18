"Routing table to dispatch graphql calls to the correct handler"
import importlib

# graphql field -> python handler
cache = {}


def clear():
    cache.clear()


def register(field):
    "Decorator to register a handler for an appsync graphql field"

    def inner(func):
        cache[field] = func
        return func

    return inner


def get_handler(field):
    return cache.get(field)


def discover(path):
    cache.clear()
    # registers handlers in the routing table as a side effect of importing
    # add more imports here as handlers are spread across files
    importlib.import_module(path)
