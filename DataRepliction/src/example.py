

"""
Example of a problem instance:


items = {
    'file1': Item(10, 'site1'),
    'file2': Item(100, 'site3')
}

sites = {
    'site1': 1000,
    'site2': 900,
    'site3': 500,
}

links = {
    ('site1', 'site2'): 11,
    ('site3', 'site2'): 20,
}

reads = {
    ('site1', 'file1'): 3,
    ('site2', 'file2'): 1,
}

writes = {
    ('site1', 'file2'): 1,
    ('site3', 'file1'): 2,
}



Example replication scheme:

replicas = {
    'file1': {'site1', 'site2'},
    'file2': {'site3'}
}


"""
