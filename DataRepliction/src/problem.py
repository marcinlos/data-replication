
from util import DictView
from replication import minimalReplication
from operator import attrgetter
from _collections import defaultdict


class Problem(object):
    ''' Full, convenient description of the replication data, with auxilary
    methods.
    '''
    def __init__(self, sites, items, reads, writes, cost):
        self.__sites = sites
        self.item_info = items
        self.sites = tuple(sites)
        self.items = tuple(items)
        self.reads = reads
        self.writes = writes
        self.cost = cost

        self.size_view = DictView(items, attrgetter('size'))
        self.primary_view = DictView(items, attrgetter('primary'))

    @property
    def nsites(self):
        return len(self.__sites)

    @property
    def nitems(self):
        return len(self.item_info)

    @property
    def capacity(self):
        return self.__sites

    @property
    def size(self):
        return self.size_view

    @property
    def primary(self):
        return self.primary_view


class Replication(object):

    def __init__(self, problem, replicas=None):
        self.data = problem

        if not replicas:
            replicas = minimalReplication(problem.item_info)

        self.__replicas = replicas
        self.closest = self.__findClosestReplicas()

    @property
    def replicas(self):
        return self.__replicas

    def verify(self):
        used = defaultdict(int)
        for item in self.data.items:
            primary = self.data.primary[item]
            size = self.data.size[item]
            if not primary in self.replicas[item]:
                self.__raisePrimaryCopyRemoved(item, primary)
            for site in self.replicas[item]:
                used[site] += size
                if used[site] > self.data.capacity[site]:
                    self.__raiseSiteOverloaded(site, used)

    def __raisePrimaryCopyRemoved(self, item, primary):
        fmt = 'Item {} removed fom primary site {}'
        msg = fmt.format(item, primary)
        raise Exception(msg)

    def __raiseSiteOverloaded(self, site, used):
        fmt = 'Site {} overloaded - max {}, total {}'
        capacity = self.data.capacity[site]
        msg = fmt.format(site, capacity, used[site])
        raise Exception(msg)

    def __findClosestReplicas(self):
        closest = {}
        p = self.data

        for site in p.sites:
            for item in self.data.items:
                closest[site, item] = p.primary[item]

                for replica in self.__replicas[item]:
                    best = closest[site, item]
                    if p.cost[site, replica] < p.cost[site, best]:
                        closest[site, item] = replica
        return closest

    def totalCost(self):
        total = 0
        p = self.data
        for (site, item), count in self.data.reads.iteritems():
            replica = self.closest[site, item]
            size = p.size[item]
            total += count * size * p.cost[site, replica]

        for (site, item), count in p.writes.iteritems():
            primary = p.primary[item]
            size = p.size[item]

            for replica in self.replicas[item] | {site}:
                total += count * size * p.cost[replica, primary]

        return total
