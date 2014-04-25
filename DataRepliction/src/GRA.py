
from replication import minimalReplication
from replication import totalCost
from replication import checkConstraints
from replication import closestReplicas
from _collections import defaultdict
from random import randint
from SRA import SRA
from pyevolve import Util


class RandomizedSRA(SRA):

    def __init__(self, *args, **kwargs):
        super(RandomizedSRA, self).__init__(*args, **kwargs)

    def siteIterator(self, possible):
        def randSeq():
            while True:
                n = len(possible)
                idx = randint(0, n - 1)
                yield idx, possible[idx]
        return randSeq()


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
        
        self.mutation_prob = 0.1
    
    def eval(self, genome):
        replicas = self.genomeToReplicas(genome)
        return self.totalCost(replicas)

    def initialize(self, genome, **kwargs):
        sra = RandomizedSRA(self.sites, self.cost, self.items, self.reads,
            self.writes)
        replicas = sra.run()
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
        self.zeroGenome(genome)

        for item, sites in replicas.iteritems():
            i = self.item_lookup[item]
            for site in sites:
                s = self.site_lookup[site]
                genome.setItem(s, i, 1)

    def zeroGenome(self, genome):
        for i in xrange(genome.getHeight()):
            for j in xrange(genome.getWidth()):
                genome.setItem(i, j, 0)

    def mutate(self, genome, **kwargs):
        p = self.mutation_prob
        height, width = genome.getSize()
        mutations = 0

        for i in xrange(height):
            for j in xrange(width):
                if Util.randomFlipCoin(p):
                    prev = genome[i][j]
                    genome.setItem(i, j, 1 - prev)
                    mutations += 1
        return mutations

