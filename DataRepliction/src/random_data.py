
import random
from replication import Item
from _collections import defaultdict
from collections import Counter
from replication import costMatrix
from problem import Problem


def randIntGen(a, b):
    return lambda: random.randint(a, b)


def randomLinks(V, avg_degree=5, min_weight=1, max_weight=5):
    """ Creates random connected directed weighted graph with vertex set V,
    weights between min_weight and max_weight, and given average degree.

    Distribution of this random variable is far from uniform on the set of all
    connected graphs. Vertices that appear earlier on the list are likely
    to have higher degree than those that follow.

    Warning: it's O(|V|^2) for fixed density, but performance degrades severly
    as average degree approaches |V| (as for hash table and load factor)
    """
    V = list(V)
    n = len(V)
    E = {}
    total_edges = n * avg_degree / 2

    used = [V[0]]

    rand_weight = randIntGen(min_weight, max_weight)

    for v in V[1:]:
        u = random.choice(used)
        E[u, v] = rand_weight()
        used.append(v)

    edge_count = len(V)

    while edge_count < total_edges:
        u, v = random.sample(V, 2)
        if (u, v) not in E:
            E[u, v] = rand_weight()
            edge_count += 1

    return E


def randomSites(n, min_capacity=500, max_capacity=2000):
    rand_capacity = randIntGen(min_capacity, max_capacity)
    return {'site_{}'.format(k): rand_capacity() for k in xrange(1, n + 1)}


def randomItems(n, capacity, min_size=10, max_size=60):
    """ Generates items - sizes and primary sites. May fail even in cases when
    it's possible to find item list subject to specified constraints.

    capacity : map (site name -> site capacity)
    """
    rand_size = randIntGen(min_size, max_size)
    used = defaultdict(int)
    site_list = list(capacity)
    items = {}

    attempts = 0
    i = 0
    while i < n:
        size = rand_size()
        site = random.choice(site_list)
        if used[site] + size < capacity[site]:
            i += 1
            used[site] += size
            name = 'item_{}'.format(i)
            items[name] = Item(size, site)
        else:
            attempts += 1

            # Since we haven't even started considering replicas, and we are
            # already out of space, it doesn't make much sense to proceed
            if attempts > 100:
                raise Exception('Couldn\'t add item in 100 attempts')

    return items


def randomTraffic(n, sites, items):
    traffic = Counter()

    sites = list(sites)
    items = list(items)

    for _ in xrange(n):
        site = random.choice(sites)
        item = random.choice(items)
        traffic[site, item] += 1

    return traffic
