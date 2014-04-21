
import random


def randomLinks(V, density=0.2, min_weight=1, max_weight=5):
    """ Creates random connected directed weighted graph with vertex set V,
    weights between min_weight and max_weight, and given density (i.e. fraction
    of all edges).

    Distribution of this random variable is far from uniform on the set of all
    connected graphs. Vertices that appear earlier on the list are likely
    to have higher degree than those that follow.

    Warning: it's O(|V|^2) for fixed density, but performance degrades severly
    as density approaches 1 (as for hash table and load factor)
    """
    V = list(V)
    n = len(V)
    E = {}
    total_edges = density * (n * (n - 1)) / 2

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
