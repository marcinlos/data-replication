from _collections import defaultdict

import sys

items = {
    'file1': 10,
    'file2': 100
}

sites = {
    'site1': 1000,
    'site2': 900,
    'site3': 500,
}

links = {
    ('site1', 'site2'): 11,
    ('site3', 'site2'): 20,
}


def computeCostMatrix(V, E):
    cost = defaultdict(lambda: sys.maxint)

    for v in V:
        cost[v, v] = 0

    for u, v in E:
        cost[u, v] = cost[v, u] = E[u, v]

    for u in V:
        for v in V:
            for w in V:
                if cost[v, w] > cost[v, u] + cost[u, w]:
                    cost[v, w] = cost[v, u] + cost[u, w]
    return dict(cost)


if __name__ == '__main__':
    print computeCostMatrix(sites, links)
