
from util import Stopwatch

from replication import costMatrix, minimalReplication, totalCost,\
    checkConstraints
from replication import closestReplicas

from random_data import randomLinks, randomSites, randomItems, randomTraffic

from problem import Problem, Replication
from SRA import SRA


if __name__ == '__main__':
    t = Stopwatch()

    t.start('data creation')
    sites = randomSites(30, min_capacity=2000)
    links = randomLinks(sites, avg_degree=4)
    cost = costMatrix(sites, links)
    items = randomItems(100, sites, max_size=20)

    rwRatio = 0.01
    readCount = 100000
    writeCount = int(readCount * rwRatio)

    reads = randomTraffic(readCount, sites, items)
    writes = randomTraffic(writeCount, sites, items)

    problem = Problem(sites, items, reads, writes, cost)

    t.stop('data creation')

    minimal = Replication(problem)
    base_cost = minimal.totalCost()

    t.start('constructor')
    sra = SRA(problem)
    t.stop('constructor')

    t.start('run')
    replicas = sra.run()
    t.stop('run')

    checkConstraints(replicas, items, sites)
    solution = Replication(problem, replicas)
    final_cost = solution.totalCost()

    change = 1 - float(final_cost) / base_cost
    print 'Initial cost:    ', base_cost
    print 'Final cost:      ', final_cost
    print 'Improvement:      {:.2%}'.format(change)
    print

    print 'Data creation: {}s'.format(t['data creation'])
    print

    print 'Results:'
    print '  Preprocessing: {:.5}s'.format(t['constructor'])
    print '  Computation:   {:.5}s'.format(t['run'])
    print
