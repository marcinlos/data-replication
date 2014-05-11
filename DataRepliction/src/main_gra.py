
from replication import costMatrix
from random_data import randomLinks, randomSites, randomItems, randomTraffic

from pyevolve import GSimpleGA
from pyevolve import Selectors
from pyevolve import Consts

from SRA import SRA
from GRA import GRA, ReplicationGenome
from problem import Problem, Replication
from ui import printDetails

import article_random_data as gen


def analyze(replication, prefix, result):
    problem = replication.data

    replication.verify()

    minimal = Replication(problem)
    base_cost = minimal.totalCost()
    final_cost = replication.totalCost()
    change = 1 - float(final_cost) / base_cost
    result[prefix + '_final_cost'] = final_cost
    result[prefix + '_improvement'] = change
    count = replication.replicasCount()
    result[prefix + '_replicas'] = count


def makeProblem(item_count, site_count, U, C):
    site_names = gen.randomSites(site_count)
    items = gen.randomItems(item_count, site_names)
    sites = gen.randomCapacities(site_names, items, C)
    reads, writes = gen.randomTraffic(sites, items, U)
    cost = gen.randomNetwork(sites)

    return Problem(sites, items, reads, writes, cost)


def runTest(item_count, site_count, U, C, iters):
    res = {}

    problem = makeProblem(item_count, site_count, U, C)
    gra = GRA(problem)

    minimal = Replication(problem)
    base_cost = minimal.totalCost()

    print 'Initial cost:', base_cost
    res['base_cost'] = base_cost

    genome = ReplicationGenome(problem)
    genome.setRules(gra)

    ga = GSimpleGA.GSimpleGA(genome)

    ga.setMinimax(Consts.minimaxType['minimize'])
    ga.selector.set(Selectors.GRouletteWheel)
    ga.setGenerations(iters)

    ga.evolve(freq_stats=1)

    best = ga.bestIndividual()
    replicas = best.replicas
    solution = Replication(problem, replicas)

    print 'GRA:'
    printDetails(solution)
    analyze(solution, 'gra', res)

    sra = SRA(problem)
    replicas = sra.run()
    solution = Replication(problem, replicas)

    print 'SRA:'
    printDetails(solution)
    analyze(solution, 'sra', res)
    return res


if __name__ == '__main__':

    item_count = 1000
    site_count = 100
    U = 0.05
    C = 0.15
    iters = 5

    res = runTest(item_count, site_count, U, C, iters)
    print res
