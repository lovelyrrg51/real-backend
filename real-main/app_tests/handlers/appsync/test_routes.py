import pytest

from app.handlers.appsync import routes


@pytest.fixture(autouse=True)
def before():
    routes.clear()


def test_graphql_adds_to_it():
    @routes.register('Mytype.myfield')
    def myfunc():
        pass

    assert routes.cache == {'Mytype.myfield': myfunc}


def test_clear_works():
    @routes.register('Mytype.myfield')
    def myfunc():
        pass

    assert routes.cache == {'Mytype.myfield': myfunc}
    routes.clear()
    assert routes.cache == {}


def test_discover():
    assert routes.cache == {}

    routes.discover('app_tests.handlers.appsync.mock_handlers')
    from app_tests.handlers.appsync import mock_handlers  # import must happen after routes.discover()

    assert routes.cache == {
        'Type.field1': mock_handlers.handler_1,
        'Type.field2': mock_handlers.handler_2,
    }
