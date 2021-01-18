# a few handy testing utils


def pk(item):
    return {
        'partitionKey': item['partitionKey'],
        'sortKey': item['sortKey'],
    }
