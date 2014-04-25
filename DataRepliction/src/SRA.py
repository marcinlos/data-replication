

from itertools import count
from replication import minimalReplication
from _collections import defaultdict


def round_robin(seq):
    for i in count():
        idx = i % len(seq)
        yield idx, seq[idx]


class SRA(object):

    def __init__(self, problem):
        self.cost = problem.cost
        self.items = problem.item_info

        self.reads = defaultdict(lambda: defaultdict(int))
        for (site, item), count in problem.reads.iteritems():
            self.reads[site][item] = count

        self.writes = defaultdict(lambda: defaultdict(int))
        for (site, item), count in problem.writes.iteritems():
            self.writes[item][site] = count

        self.free = dict(problem.capacity)
        self.replicas = minimalReplication(problem.item_info)

        for item, replicationSites in self.replicas.iteritems():
            for s in replicationSites:
                self.free[s] -= problem.size[item]

        self.closest = {}
        for name, item in problem.item_info.iteritems():
            self.closest[name] = {site: item.primary for site in problem.sites}

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
        return round_robin(possible)

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
