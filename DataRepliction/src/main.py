from _collections import defaultdict

import sys
from collections import namedtuple

Item = namedtuple('Item', ['weight', 'primary'])

items = {
    'file1': Item(10, 'site1'),
    'file2': Item(100, 'site3')
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

reads = {
    ('site1', 'file1'): 3,
    ('site2', 'file2'): 1,
}

writes = {
    ('site1', 'file2'): 1,
    ('site3', 'file1'): 2,
}

replicas = {
    'file1': {'site1', 'site2'},
    'file2': {'site3'}
}


def findClosestReplicas(sites, items, replicas, cost):
    closest = {}

    for site in sites:
        for item in replicas:
            closest[site, item] = items[item].primary

            for replica in replicas[item]:
                best = closest[site, item]
                if cost[site, replica] < cost[site, best]:
                    closest[site, item] = replica

    return closest


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
    return cost


def computeTotalCost(reads, writes, closest, cost, items, replicas):
    total = 0
    for (site, item), count in reads.iteritems():
        replica = closest[site, item]
        weight = items[item].weight
        total += count * weight * cost[site, replica]

    for (site, item), count in writes.iteritems():
        primary = items[item].primary
        weight = items[item].weight

        for replica in replicas[item] | {site}:
            total += count * weight * cost[replica, primary]

    return total


if __name__ == '__main__':
    cost = computeCostMatrix(sites, links)
    closest = findClosestReplicas(sites, items, replicas, cost)

    for site in sites:
        print '{}:'.format(site)
        for item in items:
            best = closest[site, item]
            print '    {}: {}, cost: {}'.format(item, best, cost[site, best])

    total = computeTotalCost(reads, writes, closest, cost, items, replicas)
    print total




















