
from replication import costMatrix
from replication import minimalReplication
from replication import totalCost
from replication import checkConstraints
from replication import closestReplicas
from random_data import randomLinks, randomSites, randomItems, randomTraffic

from pyevolve import G2DBinaryString
from pyevolve import GSimpleGA
from pyevolve import Selectors
from pyevolve import DBAdapters

from GRA import GRA


if __name__ == '__main__':

    N = 30
    M = 100

    sites = randomSites(N, min_capacity=2000)
    links = randomLinks(sites, avg_degree=4)
    cost = costMatrix(sites, links)
    items = randomItems(M, sites, max_size=20)

    rwRatio = 0.05
    readCount = 100000
    writeCount = int(readCount * rwRatio)

    reads = randomTraffic(readCount, sites, items)
    writes = randomTraffic(writeCount, sites, items)

    gra = GRA(sites, items, reads, writes, cost)

    minimal = minimalReplication(items)
    closest = closestReplicas(sites, items, minimal, cost)
    base_cost = totalCost(reads, writes, closest, cost, items, minimal)

    checkConstraints(minimal, items, sites)

    print 'Initial cost:', base_cost

    genome = G2DBinaryString.G2DBinaryString(N, M)
    genome.evaluator.set(gra.eval)
    genome.initializator.set(gra.initialize)

    ga = GSimpleGA.GSimpleGA(genome)

    ga.selector.set(Selectors.GRouletteWheel)
    ga.setGenerations(500)
    ga.terminationCriteria.set(GSimpleGA.ConvergenceCriteria)

    sqlite_adapter = DBAdapters.DBSQLite(identify="ex1", resetDB=True)
    ga.setDBAdapter(sqlite_adapter)

    ga.evolve(freq_stats=20)

    print ga.bestIndividual()








