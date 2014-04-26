
from _collections import defaultdict
from random import randint
from SRA import SRA
from problem import Replication
from pyevolve import Util
from pyevolve import GenomeBase
from replication import checkConstraints, minimalReplication


class ConstraintViolation(Exception):
    pass


class ReplicationBuilder(object):

    def __init__(self, data, replicas=None):
        self.data = data
        self.__replicas = defaultdict(set)
        self.free = dict(data.capacity)

        if replicas is None:
            replicas = minimalReplication(self.data.item_info)

        for item, sites in replicas:
            for site in sites:
                self.add(item, site)

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


class SiteData(object):
    def __init__(self, site, data):
        self.site = site
        self.data = data
        self.capacity = data.capacity[site]
        self.free = self.capacity
        self.replicas = [0] * data.nitems

    def __getitem__(self, item):
        return self.replicas[item]

    def add(self, item):
        size = self.data.size[item]
        if self.free < size:
            raise ConstraintViolation('Site overloaded')
        self.free -= size
        self.replicas[item] = 1

    def remove(self, item):
        size = self.data.size[item]
        self.free += size
        self.replicas[item] = 0


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

    @staticmethod
    def crossover(a, b):
        ga = defaultdict(set)
        for item, sites in a.replicas.iteritems():
            for site in sites:
                ga[site].add(item)
        gb = defaultdict(set)
        for item, sites in b.replicas.iteritems():
            for site in sites:
                gb[site].add(item)

        data = a.data
        items = a.data.items
        sites = a.data.sites
        nitems = len(items)
        for site in sites:
            p, q = randint(0, nitems), randint(0, nitems)
            if p >= q:
                p, q = q, p
            free_a = a.free[site]
            free_b = b.free[site]

            items_in_range = [items[i] for i in xrange(p, q)]
            items_a = [item for item in items_in_range if item in ga[site]]
            items_b = [item for item in items_in_range if item in gb[site]]
            size_a = sum(data.size[item] for item in items_a)
            size_b = sum(data.size[item] for item in items_b)
            diff = size_a - size_b

            if free_a + diff >= 0 and free_b - diff >= 0:
                ga[site].difference_update(items_a)
                ga[site].update(items_b)
                gb[site].difference_update(items_b)
                gb[site].update(items_a)
            else:
                ga[site], gb[site] = gb[site], ga[site]

        replicas_a = defaultdict(set)
        for site, items in ga.iteritems():
            for item in items:
                replicas_a[item].add(site)

        replicas_b = defaultdict(set)
        for site, items in gb.iteritems():
            for item in items:
                replicas_b[item].add(site)

        new_a = a.clone()
        new_b = b.clone()
        new_a.replicas = replicas_a
        new_b.replicas = replicas_b
        return (new_a, new_b)

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
        mom = args['mom']
        dad = args['dad']

        return ReplicationGenome.crossover(mom, dad)

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

