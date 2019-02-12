#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae.connectors.mongodb import MongodbConnector


mongodb = MongodbConnector('mongodb',
                           27017,
                           default_database='catenae',
                           default_collection='catenae',
                           connect=True)

# Open and close a connection
mongodb.open_connection()
mongodb.close_connection()

item = {'identifier': 'id1'}
attributes = {'attr1': 'value1', 'attr2': 'value2','attr3':[]}

# Try to add elements to a document list
mongodb.push(item,'attr3',["value3","value4"])
result = mongodb.get(item)
for result_item in result:
    assert([x for x in result_item['attr3'] if x in ["value3","value4"]])

# Create index
mongodb.create_index('attr1', type_='desc')
mongodb.create_index(keys=[('attr1', 1)], background=False)

# Remove item if it exists
if mongodb.exists(item):
    mongodb.remove(item)

# Item does not exist
assert(not mongodb.exists(item))
result = mongodb.get(item, limit=1)
for result_item in result:
    assert(False)
assert(mongodb.count() == 0)

# Item exists
mongodb.put(item)
assert(mongodb.exists(item))
result = mongodb.get(item, limit=1)
result = next(result)
result.pop('_id')
assert(result == item)
assert(mongodb.count() == 1)
mongodb.remove(item)

# Item does not exist
assert(not mongodb.exists(item))
result = mongodb.get(item)
for result_item in result:
    assert(False)

# Item exists with attributes
mongodb.put(item, attributes)
assert(mongodb.exists(item))

result = mongodb.get(item, limit=100, index_attribute='attr1', index_type='desc')
result = next(result)
result.pop('_id')
expected_result = item
expected_result.update(attributes)
assert(result == expected_result)

result = mongodb.get(item, index_attribute='attr1', index_type='asc')
result = next(result)
result.pop('_id')
expected_result = item
expected_result.update(attributes)
assert(result == expected_result)

# Get random item
assert(next(mongodb.get_random()))
assert(next(mongodb.get_random(query={'attr1': 'value1'}, sort={'attr1': -1})))
assert(next(mongodb.get_random(sort={'attr2': -1})))
mongodb.remove(item)

print('PASSED')
