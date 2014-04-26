
from replication import checkConstraints
from problem import Replication


def printCostMatrix(sites, cost):
    print '         ',
    for v in sites:
        print '{:^6}'.format(v),
    print
    for u in sites:
        print '{:7}'.format(u),
        for v in sites:
            print '{:6}'.format(cost[u, v]),
        print


def printDetails(replication):
    problem = replication.data
    replicas = replication.replicas

    checkConstraints(replicas, problem.item_info, problem.capacity)

    minimal = Replication(problem)
    base_cost = minimal.totalCost()
    final_cost = replication.totalCost()

    change = 1 - float(final_cost) / base_cost
    print 'Initial cost:    ', base_cost
    print 'Final cost:      ', final_cost
    print 'Improvement:      {:.2%}'.format(change)
