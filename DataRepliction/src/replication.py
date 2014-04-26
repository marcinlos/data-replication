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


def totalCost(reads, writes, closest, cost, items, replicas):
    total = 0
    for (site, item), count in reads.iteritems():
        replica = closest[site, item]
        size = items[item].size
        total += count * size * cost[site, replica]

    for (site, item), count in writes.iteritems():
        primary = items[item].primary
        size = items[item].size

        for replica in replicas[item] | {site}:
            total += count * size * cost[replica, primary]

    return total


def checkConstraints(replicas, items, capacity):
    used = defaultdict(int)
    for item, info in items.iteritems():
        primary = info.primary
        size = info.size
        if not primary in replicas[item]:
            raise Exception('Item {} removed fom primary site {}'
                .format(item, primary))
        for site in replicas[item]:
            used[site] += size
            if used[site] > capacity[site]:
                fmt = 'Site {} overloaded - max {}, total {}'
                msg = fmt.format(site, capacity[site], used[site])
                raise Exception(msg)


def closestReplicas(sites, items, replicas, cost):
    closest = {}

    for site in sites:
        for item in items:
            closest[site, item] = items[item].primary

            for replica in replicas[item]:
                best = closest[site, item]
                if cost[site, replica] < cost[site, best]:
                    closest[site, item] = replica

    return closest


def totalUsedStorage(items, replicas):
    used = defaultdict(int)
    for item, sites in replicas.iteritems():
        size = items[item].size
        for site in sites:
            used[site] += size

    return used


def minimalReplication(items):
    return {name: {item.primary} for name, item in items.iteritems()}
