
from replication import minimalReplication
from replication import totalCost
from replication import checkConstraints
from replication import closestReplicas
from _collections import defaultdict


class GRA(object):

    def __init__(self, sites, items, reads, writes, cost):
        self.items = items
        self.sites = sites
        self.reads = reads
        self.writes = writes
        self.cost = cost
        self.site_names = tuple(sites)
        self.item_names = tuple(items)

        self.site_lookup = {name: i for i, name in enumerate(sites)}
        self.item_lookup = {name: i for i, name in enumerate(items)}

    def eval(self, genome):
        replicas = self.genomeToReplicas(genome)
        return self.totalCost(replicas)

    def initialize(self, genome, **kwargs):
        replicas = minimalReplication(self.items)
        self.replicasToGenome(replicas, genome)

    def genomeToReplicas(self, genome):
        replicas = defaultdict(set)
        for s in xrange(len(self.sites)):
            site = self.site_names[s]
            for i in xrange(len(self.items)):
                item = self.item_names[i]
                if genome.getItem(s, i) == 1:
                    replicas[item].add(site)
        return replicas

    def totalCost(self, replicas):
        closest = closestReplicas(self.sites, self.items, replicas, self.cost)
        return totalCost(self.reads, self.writes, closest, self.cost,
            self.items, replicas)

    def replicasToGenome(self, replicas, genome):
        genome.clearString()
        for item, sites in replicas.iteritems():
            i = self.item_lookup[item]
            for site in sites:
                s = self.site_lookup[site]
                genome.setItem(s, i, 1)
