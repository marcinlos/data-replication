
from _collections import defaultdict
from random import randint
from SRA import SRA
from problem import Replication
from pyevolve import Util


class RandomizedSRA(SRA):

    def __init__(self, problem):
        super(RandomizedSRA, self).__init__(problem)

    def siteIterator(self, possible):
        def randSeq():
            while True:
                n = len(possible)
                idx = randint(0, n - 1)
                yield idx, possible[idx]
        return randSeq()


class GRA(object):

    def __init__(self, problem):
        self.data = problem
        self.mutation_prob = 0.1

    def eval(self, genome):
        replicas = self.genomeToReplicas(genome)
        return self.totalCost(replicas)

    def initialize(self, genome, **kwargs):
        sra = RandomizedSRA(self.data)
        replicas = sra.run()
        self.replicasToGenome(replicas, genome)

    def genomeToReplicas(self, genome):
        replicas = defaultdict(set)
        for s in xrange(self.data.nsites):
            site = self.data.sites[s]
            for i in xrange(self.data.nitems):
                item = self.data.items[i]
                if genome.getItem(s, i) == 1:
                    replicas[item].add(site)
        return replicas

    def totalCost(self, replicas):
        rep = Replication(self.data, replicas)
        return rep.totalCost()

    def replicasToGenome(self, replicas, genome):
        self.zeroGenome(genome)

        for item, sites in replicas.iteritems():
            i = self.data.item_idx[item]
            for site in sites:
                s = self.data.site_idx[site]
                genome.setItem(s, i, 1)

    def zeroGenome(self, genome):
        for s in self.data.site_range:
            for i in self.data.item_range:
                genome.setItem(s, i, 0)

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

