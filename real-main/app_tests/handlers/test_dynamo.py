from unittest.mock import Mock

from app.handlers.dynamo.dispatch import DynamoDispatch


def test_dynamo_dispatch_pk_sk_prefixes():
    dispatch = DynamoDispatch()

    f1 = Mock()
    dispatch.register('pkpre1', 'skpre1', ['INSERT'], f1)
    assert dispatch.search('pkpre2', 'skpre1', 'INSERT', {}, {}) == []
    assert dispatch.search('pkpre1', 'skpre2', 'INSERT', {}, {}) == []
    assert dispatch.search('pkpre1', 'skpre1', 'INSERT', {}, {}) == [f1]

    f2 = Mock()
    dispatch.register('pkpre1', 'skpre2', ['INSERT'], f2)
    assert dispatch.search('pkpre2', 'skpre1', 'INSERT', {}, {}) == []
    assert dispatch.search('pkpre1', 'skpre2', 'INSERT', {}, {}) == [f2]
    assert dispatch.search('pkpre1', 'skpre1', 'INSERT', {}, {}) == [f1]

    f3 = Mock()
    dispatch.register('pkpre1', 'skpre2', ['INSERT'], f3)
    assert dispatch.search('pkpre2', 'skpre1', 'INSERT', {}, {}) == []
    assert dispatch.search('pkpre1', 'skpre2', 'INSERT', {}, {}) == [f2, f3]
    assert dispatch.search('pkpre1', 'skpre1', 'INSERT', {}, {}) == [f1]

    f4 = Mock()
    dispatch.register('pkpre4', 'skpre2', ['INSERT'], f4)
    assert dispatch.search('pkpre2', 'skpre1', 'INSERT', {}, {}) == []
    assert dispatch.search('pkpre1', 'skpre2', 'INSERT', {}, {}) == [f2, f3]
    assert dispatch.search('pkpre1', 'skpre1', 'INSERT', {}, {}) == [f1]


def test_dynamo_dispatch_event_names():
    dispatch = DynamoDispatch()

    f1 = Mock()
    dispatch.register('pkpre', 'skpre', ['INSERT'], f1)
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {}, {}) == [f1]
    assert dispatch.search('pkpre', 'skpre', 'MODIFY', {}, {}) == []
    assert dispatch.search('pkpre', 'skpre', 'REMOVE', {}, {}) == []

    f2 = Mock()
    dispatch.register('pkpre', 'skpre', ['INSERT', 'MODIFY'], f2)
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {}, {}) == [f1, f2]
    assert dispatch.search('pkpre', 'skpre', 'MODIFY', {}, {}) == [f2]
    assert dispatch.search('pkpre', 'skpre', 'REMOVE', {}, {}) == []

    f3 = Mock()
    dispatch.register('pkpre', 'skpre', ['INSERT', 'MODIFY', 'REMOVE'], f3)
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {}, {}) == [f1, f2, f3]
    assert dispatch.search('pkpre', 'skpre', 'MODIFY', {}, {}) == [f2, f3]
    assert dispatch.search('pkpre', 'skpre', 'REMOVE', {}, {}) == [f3]

    f4 = Mock()
    dispatch.register('pkpre', 'skpre', ['MODIFY', 'REMOVE'], f4)
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {}, {}) == [f1, f2, f3]
    assert dispatch.search('pkpre', 'skpre', 'MODIFY', {}, {}) == [f2, f3, f4]
    assert dispatch.search('pkpre', 'skpre', 'REMOVE', {}, {}) == [f3, f4]

    f5 = Mock()
    dispatch.register('pkpre', 'skpre', ['REMOVE'], f5)
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {}, {}) == [f1, f2, f3]
    assert dispatch.search('pkpre', 'skpre', 'MODIFY', {}, {}) == [f2, f3, f4]
    assert dispatch.search('pkpre', 'skpre', 'REMOVE', {}, {}) == [f3, f4, f5]


def test_dynamo_dispatch_attributes():
    dispatch = DynamoDispatch()

    f1 = Mock()
    dispatch.register('pkpre', 'skpre', ['INSERT'], f1, {'k1': 0})
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {}, {}) == []
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {}, {'k1': 0}) == []
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {'k1': 0}, {}) == []
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {'k1': 0}, {'k1': 0}) == []
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {'k1': 0}, {'k1': 2}) == [f1]
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {'k1': 4}, {}) == [f1]

    f2 = Mock()
    dispatch.register('pkpre', 'skpre', ['INSERT'], f2, {'k1': 0, 'k2': None})
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {}, {}) == []
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {}, {'k2': None}) == []
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {'k2': 'yup'}, {}) == [f2]
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {'k1': 0}, {'k1': 0}) == []
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {'k1': 0}, {'k1': 2}) == [f1, f2]
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {'k1': 4}, {}) == [f1, f2]
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {'k1': 4}, {'k2': 'arg'}) == [f1, f2]
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {'k1': 4}, {'k2': 0}) == [f1, f2]

    f3 = Mock()
    dispatch.register('pkpre', 'skpre', ['INSERT'], f3, {'k3': 'd'})
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {}, {}) == []
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {}, {'k3': 'd'}) == []
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {'k3': ''}, {}) == [f3]
    assert dispatch.search('pkpre', 'skpre', 'INSERT', {'k3': 42}, {}) == [f3]
