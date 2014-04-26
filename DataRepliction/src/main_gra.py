
from replication import costMatrix
from random_data import randomLinks, randomSites, randomItems, randomTraffic

from pyevolve import GSimpleGA
from pyevolve import Selectors
from pyevolve import Consts

from SRA import SRA
from GRA import GRA, ReplicationGenome
from problem import Problem, Replication
from ui import printDetails


if __name__ == '__main__':

    N = 10
    M = 40

    sites = randomSites(N, min_capacity=100, max_capacity=300)
    links = randomLinks(sites, avg_degree=4)
    cost = costMatrix(sites, links)
    items = randomItems(M, sites, max_size=20)

    rwRatio = 0.01
    readCount = 100000
    writeCount = int(readCount * rwRatio)

    reads = randomTraffic(readCount, sites, items)
    writes = randomTraffic(writeCount, sites, items)
    problem = Problem(sites, items, reads, writes, cost)

    gra = GRA(problem)

    minimal = Replication(problem)
    base_cost = minimal.totalCost()

    print 'Initial cost:', base_cost

    genome = ReplicationGenome(problem)
    genome.evaluator.set(gra.eval)
    genome.initializator.set(gra.initialize)
    genome.mutator.set(gra.mutate)
    genome.crossover.set(gra.crossover)

    ga = GSimpleGA.GSimpleGA(genome)

    ga.setMinimax(Consts.minimaxType['minimize'])
    ga.selector.set(Selectors.GRouletteWheel)
    ga.setGenerations(10)

    ga.evolve(freq_stats=1)

    best = ga.bestIndividual()
    replicas = best.replicas
    solution = Replication(problem, replicas)

    print 'GRA:'
    printDetails(solution)

    sra = SRA(problem)
    replicas = sra.run()
    solution = Replication(problem, replicas)
    print 'SRA:'
    printDetails(solution)








