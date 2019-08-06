#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae.connectors.mongodb import MongodbConnector


class MongoTest:
    def __init__(self):
        self.mongodb = MongodbConnector('mongodb', 27017, connect=True)
        self.mongodb.set_defaults('catenae', 'catenae')

        self.item = {'identifier': 'id1'}
        self.attributes = {
            'attr1': 'value1',
            'attr2': 'value2',
            'attr3': ['value1', 'value2'],
            'attr4': {
                'attr5': ['value1']
            }
        }

    def open_close_connection(self):
        self.mongodb.open_connection()
        self.mongodb.close_connection()

    def remove_record_if_exists(self):
        if self.mongodb.exists(self.item):
            self.mongodb.remove(self.item)

    def check_non_existing_key(self):
        self.remove_record_if_exists()
        assert (not self.mongodb.exists(self.item))
        result = self.mongodb.get(self.item)
        for result.item in result:
            assert (False)

    def check_existing_key(self):
        self.remove_record_if_exists()
        self.mongodb.put(self.item)
        assert (self.mongodb.exists(self.item))
        result = self.mongodb.get(self.item, limit=1)
        result = next(result)
        result.pop('_id')
        assert (result == self.item)
        assert (self.mongodb.count() == 1)

    def check_existing_key_with_attributes(self):
        self.remove_record_if_exists()
        self.mongodb.put(self.item, self.attributes)
        assert (self.mongodb.exists(self.item))

        result = self.mongodb.get(self.item, limit=100, index_attribute='attr1', index_type='desc')
        result = next(result)
        result.pop('_id')
        expected_result = dict(self.item)
        expected_result.update(self.attributes)
        assert (result == expected_result)

        result = self.mongodb.get(self.item, index_attribute='attr1', index_type='asc')
        result = next(result)
        result.pop('_id')
        expected_result = dict(self.item)
        expected_result.update(self.attributes)
        assert (result == expected_result)

    def get_random_item(self):
        assert (next(self.mongodb.get_random()))
        assert (next(self.mongodb.get_random(query={'attr1': 'value1'}, sort={'attr1': -1})))
        assert (next(self.mongodb.get_random(sort={'attr2': -1})))

    def check_array(self):
        self.remove_record_if_exists()
        self.mongodb.put(self.item, self.attributes)

        self.mongodb.push(self.item, 'attr3', ['value3', 'value4'])
        result = next(self.mongodb.get(self.item))
        assert (result['attr3'] == ['value1', 'value2', 'value3', 'value4'])

        self.mongodb.push(self.item, 'attr4.attr5', ['value2'])
        result = next(self.mongodb.get(self.item))
        assert (result['attr4']['attr5'] == ['value1', 'value2'])

    def create_index(self):
        self.mongodb.create_index('attr1', type_='desc')
        self.mongodb.create_index(keys=[('attr1', 1)], background=False)

    def start(self):
        self.open_close_connection()
        self.create_index()

        self.check_non_existing_key()
        self.check_existing_key()
        self.check_existing_key_with_attributes()
        self.get_random_item()
        self.check_array()

        print('PASSED')


if __name__ == '__main__':
    MongoTest().start()