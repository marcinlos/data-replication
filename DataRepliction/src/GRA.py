
from _collections import defaultdict
from random import randint
from SRA import SRA
from problem import Replication
from pyevolve import Util
from pyevolve import GenomeBase


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

    def setRules(self, rules):
        self.evaluator.set(rules.eval)
        self.initializator.set(rules.initialize)
        self.mutator.set(rules.mutate)
        self.crossover.set(rules.crossover)

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
        g.replicas = self.replicas

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


class Initializer(SRA):

    def __init__(self, problem):
        super(Initializer, self).__init__(problem)

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

    def eval(self, genome):
        return self.totalCost(genome.replicas)

    def initialize(self, genome, **kwargs):
        sra = Initializer(self.data)
        replicas = sra.run()
        genome.replicas = replicas

    def totalCost(self, replicas):
        rep = Replication(self.data, replicas)
        return rep.totalCost()

    def chooseTwoPoints(self):
        n = self.data.nitems
        p, q = randint(0, n), randint(0, n)
        if p >= q:
            p, q = q, p
        return (p, q)

    def replicasBySite(self, by_item):
        by_site = defaultdict(set)
        for item, sites in by_item.iteritems():
            for site in sites:
                by_site[site].add(item)
        return by_site

    def replicasByItem(self, by_site):
        by_item = defaultdict(set)
        for site, items in by_site.iteritems():
            for item in items:
                by_item[item].add(site)
        return by_item

    def crossover(self, genome, **args):
        a = args['mom']
        b = args['dad']

        ra = self.replicasBySite(a.replicas)
        rb = self.replicasBySite(b.replicas)

        data = self.data
        items = self.data.items
        sites = self.data.sites

        for site in sites:
            p, q = self.chooseTwoPoints()

            free_a = a.free[site]
            free_b = b.free[site]

            selected = {items[i] for i in xrange(p, q)}
            items_a = selected & ra[site]
            items_b = selected & rb[site]
            size_a = sum(data.size[item] for item in items_a)
            size_b = sum(data.size[item] for item in items_b)
            diff = size_a - size_b

            if free_a + diff >= 0 and free_b - diff >= 0:
                ra[site].difference_update(items_a)
                ra[site].update(items_b)
                rb[site].difference_update(items_b)
                rb[site].update(items_a)
            else:
                ra[site], rb[site] = rb[site], ra[site]

        new_a = a.clone()
        new_b = b.clone()
        new_a.replicas = self.replicasByItem(ra)
        new_b.replicas = self.replicasByItem(rb)
        return (new_a, new_b)

    def mutate(self, genome, **kwargs):
        p = kwargs['pmut']
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
