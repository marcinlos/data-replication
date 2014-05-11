
"""
This module is supposed to faithfully follow random data creation as described
in the original parper.
"""

import random
from random import randint
from collections import defaultdict
from replication import Item


def randomNetwork(sites, cmin=1, cmax=10):
    """
    sites: names of the sites
    """
    sites = list(sites)
    c = {}
    for i in xrange(len(sites)):
        a = sites[i]
        c[a, a] = 0
        for j in xrange(i + 1, len(sites)):
            b = sites[j]
            c[b, a] = c[a, b] = randint(cmin, cmax)
    return c


def uniform(base, factor):
    """ Returns a random integer with uniform distribution on
    [ base*factor/2, 3 * base*factor/2]
    """
    b = factor * base
    c = int(b / 2)
    return randint(c, 3 * c)


def randomTraffic(sites, items, update_fraction, min_reads=1, max_reads=40):
    """ Generates random traffic description - pair of dictionaries
    (reads, writes) with mapping (site, item) -> count

    sites: names of the sites
    items: names of the items
    update_fraction: value in [0, 1] determining desired amount of updates
        among all the updates
    min_reads: minimum number of reads per (site, object) pair
    max_reads: maximum number of reads per (site, object) pair
    """
    sites = tuple(sites)
    reads = {
        (site, item): randint(1, max_reads)
        for site in sites
        for item in items
    }
    total = defaultdict(int)
    for (site, item), count in reads.iteritems():
        total[item] += count

    updates = defaultdict(int)
    for item in items:
        count = uniform(total[item], update_fraction)
        for _ in xrange(count):
            site = random.choice(sites)
            updates[site, item] += 1
    return reads, updates


def randomItems(n, sites, mean=35, difference=20):
    """ Generates item names

    n: number of items to generate
    mean: mean of the size distribution
    difference: maximum deviation from the mean
    """
    m = mean - difference
    M = mean + difference
    return {
        'item_{}'.format(i): Item(randint(m, M), random.choice(sites))
        for i in xrange(n)
    }


def randomSites(n):
    """ Generates site names

    n: number of sites to generate
    """
    return ['site_{}'.format(i) for i in xrange(n)]


def randomCapacities(sites, items, capacity_factor=0.15):
    """ Generates random site capacities

    sites: site names
    items: full item dictionary name -> Item
    capacity_factor: determines the capacities relative to sum of item sizes
    """
    total_obj_size = sum(item.size for item in items.values())
    print 'Total size:', total_obj_size

    b = capacity_factor * total_obj_size
    c = int(b / 2)
    print 'Min capacity:', c
    print 'Max capacity:', 3 * c
    return {
        site: uniform(total_obj_size, capacity_factor)
        for site in sites
    }
