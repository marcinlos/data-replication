'''
Created on Apr 21, 2014

@author: los
'''
import unittest
from replication import *


class Test(unittest.TestCase):

    def testName(self):
        sites = {'a', 'b', 'c', 'd'}
        links = {
            ('a', 'b'): 1,
            ('a', 'd'): 1,
            ('b', 'c'): 2,
            ('b', 'd'): 3,
        }
        cost = costMatrix(sites, links)
        self.assertEqual(cost['a', 'a'], 0)
        self.assertEqual(cost['a', 'b'], 1)
        self.assertEqual(cost['a', 'c'], 3)
        self.assertEqual(cost['a', 'd'], 1)
        self.assertEqual(cost['b', 'a'], 1)
        self.assertEqual(cost['b', 'b'], 0)
        self.assertEqual(cost['b', 'c'], 2)
        self.assertEqual(cost['b', 'd'], 2)
        self.assertEqual(cost['c', 'a'], 3)
        self.assertEqual(cost['c', 'b'], 2)
        self.assertEqual(cost['c', 'c'], 0)
        self.assertEqual(cost['c', 'd'], 4)
        self.assertEqual(cost['d', 'a'], 1)
        self.assertEqual(cost['d', 'b'], 2)
        self.assertEqual(cost['d', 'c'], 4)
        self.assertEqual(cost['d', 'd'], 0)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()