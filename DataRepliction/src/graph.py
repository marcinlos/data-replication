
import random


def randomConnectedGraph(V, sparseness=0.2, min_weight=1, max_weight=5):
    V = list(V)
    n = len(V)
    E = {}
    total_edges = sparseness * (n * (n - 1)) / 2

    used = [V[0]]

    rand_weight = lambda: random.randint(min_weight, max_weight)

    for v in V[1:]:
        u = random.choice(used)
        E[u, v] = rand_weight()
        used.append(v)

    edge_count = len(V)

    while edge_count < total_edges:
        u, v = random.sample(V, 2)
        if (u, v) not in E:
            E[u, v] = rand_weight()
            edge_count += 1

    return E
