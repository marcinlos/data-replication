
import replication
from itertools import count
from replication import minimalReplication, closestReplicas
from _collections import defaultdict


def roundRobin(seq):
    for i in count():
        yield seq[i % len(seq)]


class SRA(object):

    def __init__(self, sites, cost, items, reads, writes):
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
                if self.size(item) < free:
                    possible_replications[site].add(item)

        as_list = list(possible_replications)
        site_iter = roundRobin(as_list)

        while possible_replications:
            site = site_iter.next()
            max_benefit = 0
            best_item = None

            fitting_items = possible_replications[site]

            for item in set(fitting_items):
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
                self.replicas[item].add(site)
                for s in possible_replications:
                    prev = self.closest[best_item][s]
                    if self.cost[s, site] < self.cost[s, prev]:
                        self.closest[best_item][s] = site

            if not fitting_items:
                del possible_replications[site]
                as_list.remove(site)

        print self.replicas
        return self.replicas

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





