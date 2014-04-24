
from util import Stopwatch

from replication import costMatrix, minimalReplication, totalCost
from replication import closestReplicas

from random_data import randomLinks, randomSites, randomItems, randomTraffic
from SRA import SRA, SRAOpt


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

    t.stop('data creation')

    minimal = minimalReplication(items)
    closest = closestReplicas(sites, items, minimal, cost)

    baseCost = totalCost(reads, writes, closest, cost, items, minimal)

    for name, Algorithm in [('simple', SRA), ('numpy', SRAOpt)]:
        t.start('constructor_{}'.format(name))
        sra = Algorithm(sites, cost, items, reads, writes)
        t.stop('constructor_{}'.format(name))

        t.start('run_{}'.format(name))
        replicas = sra.run()
        t.stop('run_{}'.format(name))

        closest = closestReplicas(sites, items, replicas, cost)
        finalCost = totalCost(reads, writes, closest, cost, items, replicas)

        print 'Base cost (no replicas):', baseCost
        print 'Final cost:             ', finalCost

    for event in t:
        time = t[event]
        print '{}: {}s'.format(event, time)
