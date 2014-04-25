

class IndexableView(object):
    def __init__(self, method):
        self.__getitem__ = method


def index_map(seq):
    return {item: idx for idx, item in enumerate(seq)}


class Problem(object):
    ''' Full, convenient description of the replication problem, with auxilary
    methods.
    '''

    def __init__(self, sites, items, reads, writes, cost):
        self.__sites = sites
        self.__items = items
        self.sites = tuple(sites)
        self.items = tuple(items)
        self.reads = reads
        self.writes = writes
        self.cost = cost

        self.site_idx = index_map(sites)
        self.item_idx = index_map(items)

        self.size_view = IndexableView(lambda i: self.__items[i].size)
        self.primary_view = IndexableView(lambda i: self.__items[i].primary)

    @property
    def nsites(self):
        return len(self.__sites)

    @property
    def nitems(self):
        return len(self.__items)

    @property
    def capacity(self):
        return self.__sites

    @property
    def size(self):
        return self.size_view

    @property
    def primary(self):
        return self.primary_view
