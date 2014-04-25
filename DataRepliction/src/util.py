
from time import time


class IndexableView(object):
    def __init__(self, method):
        self.method = method

    def __getitem__(self, *args):
        return self.method(*args)


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
