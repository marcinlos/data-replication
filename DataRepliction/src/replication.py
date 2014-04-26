from _collections import defaultdict

import sys
from collections import namedtuple


Item = namedtuple('Item', ['size', 'primary'])


def costMatrix(sites, links):
    cost = defaultdict(lambda: sys.maxint)

    for v in sites:
        cost[v, v] = 0

    for u, v in links:
        cost[u, v] = cost[v, u] = links[u, v]

    for u in sites:
        for v in sites:
            for w in sites:
                if cost[v, w] > cost[v, u] + cost[u, w]:
                    cost[v, w] = cost[v, u] + cost[u, w]
    return cost


def minimalReplication(items):
    return {name: {item.primary} for name, item in items.iteritems()}
