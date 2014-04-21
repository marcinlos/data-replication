

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
