#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae.connectors.aerospike import AerospikeConnector
import time

connected = False
while (not connected):
    try:
        aerospike = AerospikeConnector('aerospike',
                                       3000,
                                       default_namespace='catenae',
                                       default_set='catenae',
                                       connect=True)
        connected = True
    except Exception:
        time.sleep(1)

# Open and close a connection
aerospike.open_connection()
aerospike.close_connection()

sample_key = 'sample_key'
bins = {'bin1': 'value1', 'bin2': 'value2'}

# Remove record if it exists
if aerospike.exists(sample_key):
    aerospike.remove(sample_key)

# Key does not exist
assert (not aerospike.exists(sample_key))
key, value = aerospike.get(sample_key)
assert (key == None and value == None)

# Key exists but it is not stored
aerospike.put(sample_key)
assert (aerospike.exists(sample_key))
key, value = aerospike.get(sample_key)
assert (key == None and value == 0)
aerospike.remove(sample_key)

# Key does not exist
assert (not aerospike.exists(sample_key))
key, value = aerospike.get(sample_key)
assert (key == None and value == None)

# Key exists and it is stored
aerospike.put(sample_key, store_key=True)
assert (aerospike.exists(sample_key))
key, value = aerospike.get(sample_key)
assert (key == sample_key and value == None)
aerospike.remove(sample_key)

# Key exists with bins
aerospike.put(sample_key, bins=bins)
assert (aerospike.exists(sample_key))
key, value = aerospike.get(sample_key)
assert (key == None and value == bins)
aerospike.remove(sample_key)

# Create index
aerospike.put(sample_key, bins=bins)
aerospike.create_index('bin1', type_='string', name='bin1_idx')
aerospike.get_and_close('key1')
aerospike.remove(sample_key)

print('PASSED')
