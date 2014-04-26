
from time import time


class DictView(object):
    def __init__(self, source, method):
        self.source = source
        self.method = method

    def __getitem__(self, *args):
        item = self.source.__getitem__(*args)
        return self.method(item)

    def __iter__(self):
        return iter(self.source)

    def __len__(self):
        return len(self.source)


def index_map(seq):
    return {item: idx for idx, item in enumerate(seq)}


class Stopwatch(object):

    def __init__(self):
        self.__start = {}
        self.__time = {}

    def start(self, name):
        self.__time[name] = 0
        self.__start[name] = time()

    def stop(self, name):
        t = time()
        start = self.__start[name]
        self.__time[name] += t - start

    def __getitem__(self, name):
        return self.__time[name]

    def __iter__(self):
        return iter(self.__time)
