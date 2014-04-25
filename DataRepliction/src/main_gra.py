
from replication import costMatrix
from random_data import randomLinks, randomSites, randomItems, randomTraffic

from pyevolve import G2DBinaryString
from pyevolve import GSimpleGA
from pyevolve import Selectors
from pyevolve import Consts

from SRA import SRA
from GRA import GRA
from problem import Problem, Replication


if __name__ == '__main__':

    N = 30
    M = 100

    sites = randomSites(N, min_capacity=2000)
    links = randomLinks(sites, avg_degree=4)
    cost = costMatrix(sites, links)
    items = randomItems(M, sites, max_size=20)

    rwRatio = 0.07
    readCount = 100000
    writeCount = int(readCount * rwRatio)

    reads = randomTraffic(readCount, sites, items)
    writes = randomTraffic(writeCount, sites, items)
    problem = Problem(sites, items, reads, writes, cost)

    gra = GRA(problem)

    minimal = Replication(problem)
    base_cost = minimal.totalCost()

    print 'Initial cost:', base_cost

    genome = G2DBinaryString.G2DBinaryString(N, M)
    genome.evaluator.set(gra.eval)
    genome.initializator.set(gra.initialize)
    genome.mutator.set(gra.mutate)

    ga = GSimpleGA.GSimpleGA(genome)

    ga.setMinimax(Consts.minimaxType['minimize'])
    ga.selector.set(Selectors.GRouletteWheel)
    ga.setGenerations(10)

    ga.evolve(freq_stats=1)

    best = ga.bestIndividual()
    replicas = gra.genomeToReplicas(best)
    solution = Replication(problem, replicas)
    final_cost = solution.totalCost()
    print 'Final cost (GRA):', final_cost

    sra = SRA(problem)
    replicas = sra.run()
    solution = Replication(problem, replicas)
    final_cost = solution.totalCost()
    print 'Final cost (SRA):', final_cost








