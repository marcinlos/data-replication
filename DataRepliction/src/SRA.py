
from itertools import count
from replication import minimalReplication
from _collections import defaultdict
import numpy as np


def roundRobin(seq):
    for i in count():
        idx = i % len(seq)
        yield idx, seq[idx]


class Tracer(object):

    def __init__(self):
        self.counter = defaultdict(int)

    def tick(self, name):
        self.counter[name] += 1

    def printAll(self):
        for name, count in self.counter.iteritems():
            print '{}: {}'.format(name, count)


class SRA(Tracer):

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

    def run(self):
        possible_replications = defaultdict(set)
        for site, free in self.free.iteritems():
            for item in self.items:
                if self.size(item) < free and self.items[item].primary != site:
                    possible_replications[site].add(item)

        as_list = list(possible_replications)
        site_iter = roundRobin(as_list)

        while possible_replications:
            self.tick('iters')
            _, site = site_iter.next()
            max_benefit = 0
            best_item = None

            fitting_items = possible_replications[site]

            for item in set(fitting_items):
                self.tick('items checked')
                size = self.items[item].size
                free = self.free[site]
                b = self.benefit(site, item)

                if b <= 0 or size > free:
                    fitting_items.remove(item)
                elif b > max_benefit:
                    max_benefit = b
                    best_item = item

            if best_item:
                fitting_items.remove(best_item)
                size = self.items[best_item].size
                self.free[site] -= size
                self.replicas[best_item].add(site)

                for s in possible_replications:
                    self.tick('sites updated due to replication')
                    prev = self.closest[best_item][s]
                    if self.cost[s, site] < self.cost[s, prev]:
                        self.closest[best_item][s] = site

            if not fitting_items:
                del possible_replications[site]
                as_list.remove(site)

        self.printAll()
        return self.replicas

    def size(self, item):
        return self.items[item].size

    def benefit(self, site, item):
        self.tick('benefit called')
        size = self.size(item)
        closest = self.closest[item][site]
        read_cost = size * self.cost[site, closest] * self.reads[site][item]

        primary = self.items[item].primary
        write_count = self.writes[item][site]

        write_cost = 0
        for replica in self.replicas[item] | {site}:
            write_cost += write_count * size * self.cost[replica, primary]
            self.tick('writes summed')

        update_cost = 0
        for s, count in self.writes[item].iteritems():
            update_cost += count * size * self.cost[site, s]
            self.tick('updates summed')

        return (read_cost + write_cost - update_cost) / float(size)


class SRAOpt(Tracer):
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
            self.tick('iters')
            idx, (site, fitting) = site_iter.next()
            max_benefit = 0
            best_item = None

            for item in set(fitting):
                self.tick('items checked')
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

        self.printAll()
        return self.replicasAsDict()

    def replicate(self, item, site, sites_to_update):
        size = self.size[item]
        self.free[site] -= size
        self.replicas[site, item] = 1

        for s in sites_to_update:
            self.tick('sites updated due to replication')
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

    def benefit(self, site, item):
        self.tick('benefit called')
        size = self.size[item]
        closest = self.closest[site, item]
        read_cost = size * self.cost[site, closest] * self.reads[site, item]

        primary = self.primary[item]
        write_count = self.writes[site, item]

        write_cost = 0
        #for s in self.site_indices():
        #    if self.replicas[s, item] or s == site:
        #        write_cost += write_count * size * self.cost[s, primary]

        for s in self.replication_scheme[item] | {site}:
#         for s in self.site_indices():
#             if self.replicas[s, item] or s == site:
            write_cost += write_count * size * self.cost[s, primary]
            self.tick('writes summed')

        update_cost = 0
        for s in self.site_indices():
            writes = self.writes[s, item]
            cost = self.cost[site, s]
            update_cost += writes * size * cost
            self.tick('updates summed')

        return (read_cost + write_cost - update_cost) / float(size)
