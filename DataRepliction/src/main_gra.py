
from pyevolve import GSimpleGA
from pyevolve import Selectors
from pyevolve import Consts

from SRA import SRA
from GRA import GRA, ReplicationGenome
from problem import Problem, Replication
from ui import printDetails
from util import Stopwatch
from island import CustomMPIMigrator, comm, rank, is_root

import article_random_data as gen
import sys


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
    timer = Stopwatch()

    if is_root:
        problem = makeProblem(item_count, site_count, U, C)
    else:
        problem = None
    problem = comm.bcast(problem)

    minimal = Replication(problem)
    base_cost = minimal.totalCost()

    if is_root:
#         print 'Initial cost:', base_cost
        res['base_cost'] = base_cost

    timer.start('t_gra')
    gra = GRA(problem)
    genome = ReplicationGenome(problem)
    genome.setRules(gra)

    ga = GSimpleGA.GSimpleGA(genome)
    migration = CustomMPIMigrator(gra)

    ga.setMigrationAdapter(migration)
    migration.setGAEngine(ga)
    migration.setMigrationRate(5)

    ga.setMinimax(Consts.minimaxType['minimize'])
    ga.selector.set(Selectors.GRouletteWheel)
    ga.setGenerations(iters)

    ga.evolve(freq_stats=0)

    best = ga.bestIndividual()
    replicas = best.replicas
    solution = Replication(problem, replicas)
    timer.stop('t_gra')

    if is_root:
#         print 'GRA:'
#         printDetails(solution)
        analyze(solution, 'gra', res)

    timer.start('t_sra')
    sra = SRA(problem)
    replicas = sra.run()
    solution = Replication(problem, replicas)
    timer.stop('t_sra')

    if is_root:
#         print 'SRA:'
#         printDetails(solution)
        analyze(solution, 'sra', res)
        res['t_gra'] = timer['t_gra']
        res['t_sra'] = timer['t_sra']
    return res


if __name__ == '__main__':
    item_count = int(sys.argv[1])#100
    site_count = int(sys.argv[2])#20
    U = float(sys.argv[3])#0.1
    C = float(sys.argv[4])#0.15
    iters = int(sys.argv[5])#5

    res = runTest(item_count, site_count, U, C, iters)

    if is_root:
        print
        for key, val in res.items():
            print '{}: {}'.format(key, val)
