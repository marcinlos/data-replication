
from replication import *
from random_data import randomLinks, randomSites, randomItems, randomTraffic
from ui import printCostMatrix


# Problem instance

items = {
    'file1': Item(10, 'site1'),
    'file2': Item(100, 'site3')
}

sites = {
    'site1': 1000,
    'site2': 900,
    'site3': 500,
}

links = {
    ('site1', 'site2'): 11,
    ('site3', 'site2'): 20,
}

reads = {
    ('site1', 'file1'): 3,
    ('site2', 'file2'): 1,
}

writes = {
    ('site1', 'file2'): 1,
    ('site3', 'file1'): 2,
}

# end of problem instance

replicas = {
    'file1': {'site1', 'site2'},
    'file2': {'site3'}
}


if __name__ == '__main__':
    #total = totalCost(reads, writes, closest, cost, items, replicas)

    sites = randomSites(5)
    links = randomLinks(sites, 0.5)
    cost = costMatrix(sites, links)

    items = randomItems(20, sites)
    reads = randomTraffic(100, sites, items)
    writes = randomTraffic(10, sites, items)

#    closest = closestReplicas(sites, items, replicas, cost)
    printCostMatrix(sites, cost)
    print items
