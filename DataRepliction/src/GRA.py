
from _collections import defaultdict
from random import randint
from SRA import SRA
from problem import Replication
from pyevolve import Util
from pyevolve import GenomeBase
from replication import checkConstraints


class ConstraintViolation(Exception):
    pass


class ReplicationGenome(GenomeBase.GenomeBase, object):

    def __init__(self, problem):
        object.__init__(self)
        GenomeBase.GenomeBase.__init__(self)
        self.data = problem
        self.__replicas = defaultdict(set)
        self.free = dict(problem.capacity)

        for item in self.data.items:
            primary = self.data.primary[item]
            self.add(item, primary)

    @property
    def replicas(self):
        return self.__replicas

    @replicas.setter
    def replicas(self, value):
        self.clear()
        for item, sites in value.iteritems():
            for site in sites:
                self.add(item, site)

    def copy(self, g):
        GenomeBase.GenomeBase.copy(self, g)
        g.data = self.data
        #g.__replicas = defaultdict(set, self.__replicas)
        g.replicas = self.replicas
        #g.free = dict(self.free)

    def clone(self):
        other = ReplicationGenome(self.data)
        self.copy(other)
        return other

    def flip(self, site, item):
        if self.isReplicated(item, site):
            self.remove(item, site)
        else:
            self.add(item, site)

    def isReplicated(self, item, site):
        return site in self.replicas[item]

    def add(self, item, site):
        free = self.free[site]
        size = self.data.size[item]
        if free < size:
            raise ConstraintViolation('Site overloaded')

        self.replicas[item].add(site)
        self.free[site] -= size

    def remove(self, item, site):
        if self.data.primary[item] == site:
            raise ConstraintViolation('Primary site constraint violation')
        size = self.data.size[item]
        self.free[site] += size
        self.replicas[item].remove(site)

    def clear(self):
        self.replicas.clear()
        self.free = dict(self.data.capacity)

    def __usage(self):
        used = defaultdict(int)
        for item, sites in self.replicas.iteritems():
            for site in sites:
                used[site] += self.data.size[item]
        return used

    def __checkFree(self):
        usage = self.__usage()
        for site in self.data.sites:
            if self.free[site] != self.data.capacity[site] - usage[site]:
                print 'Inconsistency detected:'
                print '  Site:', site
                print '  Free:', self.free[site]
                print '  Capacity:', self.data.capacity[site]
                print '  Used:', usage[site]
                raise Exception('Inconsistency')


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
        return self.totalCost(genome.replicas)

    def initialize(self, genome, **kwargs):
        sra = RandomizedSRA(self.data)
        replicas = sra.run()
        genome.replicas = replicas

    def totalCost(self, replicas):
        rep = Replication(self.data, replicas)
        return rep.totalCost()

    def crossover(self, genome, **args):
        sister = None
        brother = None
        mom = args["mom"]
        dad = args["dad"]

        brother = dad.clone()
        sister = mom.clone()
        return (sister, brother)

    def mutate(self, genome, **kwargs):
        p = self.mutation_prob
        mutations = 0

        for site in self.data.sites:
            for item in self.data.items:
                if Util.randomFlipCoin(p):
                    try:
                        genome.flip(site, item)
                        mutations += 1
                    except ConstraintViolation:
                        pass
        return mutations

