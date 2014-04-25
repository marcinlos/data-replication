
from util import index_map, IndexableView
from replication import minimalReplication
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

        self.site_idx = index_map(sites)
        self.item_idx = index_map(items)

        self.size_view = IndexableView(lambda i: self.item_info[i].size)
        self.primary_view = IndexableView(lambda i: self.item_info[i].primary)

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

    @property
    def item_range(self):
        return xrange(self.nitems)

    @property
    def site_range(self):
        return xrange(self.nsites)


class Replication(object):

    def __init__(self, problem, replicas=None):
        self.data = problem
        self.replicas = defaultdict(set)
        #self.free = dict(problem.capacity)

        if not replicas:
            replicas = minimalReplication(problem.item_info)

        self.replicas = replicas

        self.closest = self.__findClosestReplicas()

    def __findClosestReplicas(self):
        closest = {}
        p = self.data

        for site in p.sites:
            for item in self.data.items:
                closest[site, item] = p.primary[item]

                for replica in self.replicas[item]:
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
        
        
        
        
        
        
        
        
        