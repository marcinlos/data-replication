

from itertools import count
from replication import minimalReplication
from _collections import defaultdict
import numpy as np


def roundRobin(seq):
    for i in count():
        idx = i % len(seq)
        yield idx, seq[idx]


class SRA(object):

    def __init__(self, sites, cost, items, reads, writes):
        super(SRA, self).__init__()
        self.sites = sites
        self.cost = cost
        self.items = items

        self.reads = defaultdict(lambda: defaultdict(int))
        for (site, item), count in reads.iteritems():
            self.reads[site][item] = count

        self.writes = defaultdict(lambda: defaultdict(int))
        for (site, item), count in writes.iteritems():
            self.writes[item][site] = count

        self.free = dict(sites)
        self.replicas = minimalReplication(items)

        for item, replicationSites in self.replicas.iteritems():
            for s in replicationSites:
                self.free[s] -= items[item].size

        self.closest = {}
        for name, item in items.iteritems():
            self.closest[name] = {site: item.primary for site in sites}

    def possibleReplicas(self):
        possible = []
        for site, free in self.free.iteritems():
            fitting = []
            for item in self.items:
                if self.size(item) < free and self.primary(item) != site:
                    fitting.append(item)

            possible.append((site, fitting))
        return possible

    def siteIterator(self, possible):
        return roundRobin(possible)

    def run(self):
        possible = self.possibleReplicas()
        site_iter = self.siteIterator(possible)

        while possible:
            idx, (site, fitting) = site_iter.next()
            max_benefit = 0
            best_item = None

            for item in set(fitting):
                size = self.items[item].size
                free = self.free[site]
                b = self.benefit(site, item)

                if b <= 0 or size > free:
                    fitting.remove(item)
                elif b > max_benefit:
                    max_benefit = b
                    best_item = item

            if best_item:
                fitting.remove(best_item)
                size = self.items[best_item].size
                self.free[site] -= size
                self.replicas[best_item].add(site)

                for s in (s for s, _ in possible):
                    prev = self.closest[best_item][s]
                    if self.cost[s, site] < self.cost[s, prev]:
                        self.closest[best_item][s] = site

            if not fitting:
                del possible[idx]

        return self.replicas

    def primary(self, item):
        return self.items[item].primary

    def size(self, item):
        return self.items[item].size

    def benefit(self, site, item):
        size = self.size(item)
        closest = self.closest[item][site]
        read_cost = size * self.cost[site, closest] * self.reads[site][item]

        primary = self.items[item].primary
        write_count = self.writes[item][site]

        write_cost = 0
        for replica in self.replicas[item] | {site}:
            write_cost += write_count * size * self.cost[replica, primary]

        update_cost = 0
        for s, count in self.writes[item].iteritems():
            update_cost += count * size * self.cost[site, s]

        return (read_cost + write_cost - update_cost) / float(size)


class SRAOpt(object):
    def __init__(self, sites, cost, items, reads, writes):
        super(SRAOpt, self).__init__()
        self.sites = tuple(sites)
        self.items = tuple(items)

        self.site_names = tuple(sites)
        self.item_names = tuple(items)

        self.size = tuple(items[i].size for i in self.items)
        self.free = np.fromiter((sites[s] for s in self.sites), dtype=np.int32)

        site_lookup = {site: n for n, site in enumerate(self.sites)}
        item_lookup = {item: n for n, item in enumerate(self.items)}

        self.primary = tuple(site_lookup[items[i].primary] for i in self.items)

        n = len(sites)
        m = len(items)

        self.cost = np.empty(shape=(n, n), dtype=np.int32)
        for i in xrange(n):
            for j in xrange(n):
                u, v = self.sites[i], self.sites[j]
                self.cost[i, j] = cost[u, v]

        self.reads = np.empty(shape=(n, m), dtype=np.int32)
        self.writes = np.empty(shape=(n, m), dtype=np.int32)

        for i in xrange(n):
            for j in xrange(m):
                site = self.sites[i]
                item = self.items[j]
                self.reads[i, j] = reads[site, item]
                self.writes[i, j] = writes[site, item]

        self.replicas = np.zeros(shape=(n, m), dtype=np.bool8)
        self.closest = np.empty(shape=(n, m), dtype=np.int32)

        self.replication_scheme = defaultdict(set)

        for name, item in items.iteritems():
            i = item_lookup[name]
            s = site_lookup[item.primary]
            self.replicas[s, i] = 1
            self.free[s] -= item.size
            self.closest[:, i] = s

            self.replication_scheme[item].add(site)

    def fits(self, item, site):
        return self.size[item] < self.free[site]

    def possibleNewReplicas(self):
        possible = []
        for site in self.site_indices():
            fitting = []
            for item in self.item_indices():
                if self.fits(item, site) or self.primary[item] == site:
                    fitting.append(item)
            possible.append((site, fitting))
        return possible

    def run(self):
        possible = self.possibleNewReplicas()
        site_iter = roundRobin(possible)

        while possible:
            idx, (site, fitting) = site_iter.next()
            max_benefit = 0
            best_item = None

            for item in set(fitting):
                size = self.size[item]
                free = self.free[site]
                b = self.benefit(site, item)

                if b <= 0 or size > free:
                    fitting.remove(item)
                elif b > max_benefit:
                    max_benefit = b
                    best_item = item

            if best_item is not None:
                fitting.remove(best_item)
                self.replicate(best_item, site, (s for s, _ in possible))

            if not fitting:
                del possible[idx]

        return self.replicasAsDict()

    def replicate(self, item, site, sites_to_update):
        size = self.size[item]
        self.free[site] -= size
        self.replicas[site, item] = 1

        for s in sites_to_update:
            prev = self.closest[s, item]
            if self.cost[s, site] < self.cost[s, prev]:
                self.closest[s, item] = site

        self.replication_scheme[item].add(site)

    def replicasAsDict(self):
        replication = defaultdict(set)
        for item in self.item_indices():
            item_name = self.item_names[item]
            for site in self.site_indices():
                if self.replicas[site, item]:
                    site_name = self.site_names[site]
                    replication[item_name].add(site_name)
        return replication

    def item_indices(self):
        return xrange(len(self.items))

    def site_indices(self):
        return xrange(len(self.sites))

    def write_cost(self, site, item):
        size = self.size[item]
        primary = self.primary[item]
        write_count = self.writes[site, item]

        unit_cost = np.dot(self.replicas[:, item], self.cost[:, primary])
        write_cost = write_count * unit_cost

        if self.replicas[site, item] == 0:
            write_cost += write_count * size * self.cost[site, primary]
        return write_cost

    def update_cost(self, site, item):
        unit_cost = np.dot(self.cost[site, :], self.writes[:, item])
        return unit_cost * self.size[item]

    def benefit(self, site, item):
        size = self.size[item]
        closest = self.closest[site, item]
        read_cost = size * self.cost[site, closest] * self.reads[site, item]

        write_cost = self.write_cost(site, item)
        update_cost = self.update_cost(site, item)

        return (read_cost + write_cost - update_cost) / float(size)
