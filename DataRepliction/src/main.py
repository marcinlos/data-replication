
from replication import costMatrix, minimalReplication, totalCost
from replication import closestReplicas

from random_data import randomLinks, randomSites, randomItems, randomTraffic
from ui import printCostMatrix


if __name__ == '__main__':
    sites = randomSites(100)
    links = randomLinks(sites, avg_degree=8)
    cost = costMatrix(sites, links)
    items = randomItems(1000, sites)

    rwRatio = 0.05
    readCount = 1000000
    writeCount = int(readCount * rwRatio)

    reads = randomTraffic(readCount, sites, items)
    writes = randomTraffic(writeCount, sites, items)

    minimal = minimalReplication(items)
    closest = closestReplicas(sites, items, minimal, cost)

    baseCost = totalCost(reads, writes, closest, cost, items, minimal)
    print 'Base cost (no replicas):', baseCost
